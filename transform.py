import time
from os import listdir
import pandas
import sqlite3
import argparse
import re
import icgc_fields as ifields
import generic_fields as gfields
import controlled_vocab as cvocab
import sys
import itertools


def get_key(conn, table):
    h = headers(conn, table)
    sql = 'select count(*) from {}'.format(table)
    for i in conn.execute(sql):
        total_rows = i[0]
    pieces = 1
    while pieces <= len(h):
        comp = (i for i in itertools.combinations(h, pieces))
        for x in comp:
            sql = 'select count(*) from (select distinct {} from {})'.format(', '.join(item for item in x), table)
            for count in conn.execute(sql):
                if count[0] == total_rows:
                    return x
        pieces += 1


def load(conn, file, dir):
    table = re.sub('\..*$', '', file)
    data = pandas.read_csv(dir + '/' + file, sep='\t')
    headers = [s for s in data]
    header_str = ", ".join(headers)
    sql = "create table {} ({})".format(table, header_str)
    conn.execute(sql)
    vals = ", ".join(('?' for s in data))
    sql = "insert into {} ({}) values ({})".format(table, header_str, vals)
    conn.executemany(sql, data.values)
    return


def list_tables(conn):
    table_l = []
    sql = "select name from sqlite_master where type='table' order by name;"
    tables = (table for table in conn.execute(sql))
    for table in tables:
        table_l.append(str(table[0]))
    return table_l


def headers(conn, table):
    headers = []
    for col in conn.execute('SELECT * FROM {}'.format(table)).description:
        headers.append(str(col[0]))
    return headers


def merge_tables(name, tables, drop=True):
    header = []
    for table in tables:
        for head in headers(conn, table):
            for aliases in gfields.aliases:
                if head in aliases:
                    head = aliases[0]
            if head not in header:
                header.append(head)
    h_str = ", ".join(header)
    sql = "create table {} ({})".format(name, h_str)
    conn.execute(sql)
    hlist = []
    for table in tables:
        l = []
        for head in header:
            l.append(check_merge_header(head, headers(conn, table)))
        l_str = ", ".join(l)
        hlist.append([table, l_str])
    sql = "insert into {} ({}) select {} from {} union select {} from {}".format(name, h_str, hlist[0][1], hlist[0][0],
                                                                                 hlist[1][1], hlist[1][0])
    conn.execute(sql)
    if drop:
        for table in tables:
            sql = 'drop table if exists {}'.format(table)
            conn.execute(sql)
    return name


def check_merge_header(head, header):
    if head in header:
        return head
    for aliases in gfields.aliases:
        if head in aliases:
            for alias in aliases:
                if alias in header:
                    return header[header.index(alias)]
    return "Null"


def make_query(conn, order, filetype, typedict, all_fields, req):
    fields = []
    grabbed_headers = []
    grab_tables = []
    send_fields = []
    for ord in order:
        if ord in typedict:
            table = typedict[ord]
            h = headers(conn, table)
            for i in range(len(h)):
                if h[i] in ifields.aliases:
                    k = ifields.aliases[h[i]]
                    if k in all_fields and k not in grabbed_headers:
                        grabbed_headers.append(k)
                        fields.append(table + '.' + h[i])
                        send_fields.append(k)
                        if table not in grab_tables:
                            grab_tables.append(table)
    for req_f in req:
        if req_f not in grabbed_headers and req_f == ifields.unique[filetype]:
            for table in grab_tables:
                key = get_key(conn, table)
                if len(key) == 1:
                    fields.append(key[0])
                    send_fields.append(req_f)
                elif len(key) > 1:
                    fields.append(' || '.join('ifnull(' + table + '.' + s + ', "")' for s in key))
                    send_fields.append(req_f)
    fields = ", ".join(fields)
    while len(grab_tables) > 1:
        # Note: This while is untested. It is meant to deal with the issues of pulling data from more than one table.
        grab_tables[0] = merge_tables('merge', [grab_tables[0], grab_tables[1]], drop=False)
        grab_tables.remove(grab_tables[1])
    return "select distinct {} from {}".format(fields, grab_tables[0]), ', '.join(send_fields)


def process_outfile(file, all_fields, fields_req, fields_opt):
    for i in range(len(all_fields)):
        if all_fields[i] not in file.columns.values.tolist():
            file.insert(i, all_fields[i], value='', allow_duplicates=False)
    # w2 = file.reindex(columns=all_fields)
    w2 = cvocab.specimen_type(all_fields, w2)
    w2 = cvocab.donor_sex(all_fields, w2)
    w2 = cvocab.donor_vital_status(all_fields, w2)
    for i in all_fields:
        if i in fields_req:
            if i == 'specimen_type':
                # w2[i] = w2[i].replace(to_replace=['', None], value='UNKNOWN')
                pass
            else:
                w2 = w2[pandas.notnull(w2[i])]
        elif i not in fields_opt:
            w2[i] = w2[i].replace(to_replace=['', None], value='888')
    return w2


def sql_iterator(cursor, a=5000):
    while True:
        returned = cursor.fetchmany(a)
        if not returned:
            break
        for r in returned:
            yield r


def get_main(conn, filename, dir, project, typedict, order, all_fields, req, opt):
    for filetype in order:
        if filetype in typedict.keys():
            sql = make_query(conn, order, filetype, typedict, all_fields, req)
            break
            # filetype: donor, samp, etc.
            # all_fields: the donor, samp, etc. arrays from ifields
            # req: the donor_req, etc. arrays from ifields
    if not sql:
        return False
    print filename
    sql1 = "create table {} ({})".format(filename, ', '.join(all_fields))
    conn.execute(sql1)
    sql1 = "insert into {} ({}) {}".format(filename, sql[1], sql[0])
    conn.execute(sql1)
    for samp in conn.execute('select count(*) from {}'.format(filename)):
        print samp
    cur = conn.cursor()
    sql1 = 'select {} from {}'.format('*', filename)
    cur.execute(sql1)
    k = 0
    for row in sql_iterator(cur):
        key_col = ifields.unique[filename]
        key = row[all_fields.index(ifields.unique[filename])]
        for i in range(len(all_fields)):
            change = None
            if all_fields[i] in opt and row[i] is None:
                # do nothing. This is fine.
                pass
            elif all_fields[i] in opt:
                # this depends on the opt field
                pass
            if all_fields[i] in req and row[i] is None:
                if all_fields[i] == 'specimen_type':
                    change = 'unknown'
                else:
                    #print "delete from {} where {}='{}'".format(filename, key_col, key)
                    # conn.execute('delete from {} where {}={}'.format(filename, key_col, key))
                    pass
            elif all_fields[i] not in opt:
                if row[i] is None:
                    change = '888'
            # p = False
            # if all_fields[i] == 'percentage_cellularity':
            #     print row[i]
            #     print type(row[i])
            #     try:
            #         m = int(row[i])
            #         print type(m)
            #     except TypeError as error:
            #         if
            # if all_fields[i] == 'study':
            #     if row[i] == 'PCAWG':
            #         change = '1'
            #         p = True
            #     elif row[i] is not None:
            #         change = None
            #         p = True


            if change:
                sql2 = 'update {} set {}={} where {}={}'.format(filename, all_fields[i], change, key_col, key)
              #  print sql2
                conn.execute(sql2)
    #     if k >= 5:
    #         break
    #     k += 1
    # print '\n'
    fsql = 'select {} from {}'.format('*', filename)

    # w2 = process_outfile(out, all_fields, req, opt)
    # w2.to_csv(dir + '/' + filename + '.' + project + '.txt', sep='\t', index=False)
    # print filename.capitalize() + ' file written.'
    return True


def get_three_main(conn, dir, project, typedict):
    # donor = get_main(conn, 'donor', dir, project, typedict, ('donor', 'spec', 'samp'), ifields.donor, ifields.donor_req,
    #                  ifields.donor_opt)
    spec = get_main(conn, 'spec', dir, project, typedict, ('spec', 'samp', 'donor'), ifields.spec, ifields.spec_req,
                    ifields.spec_opt)
    # samp = get_main(conn, 'samp', dir, project, typedict, ('samp', 'spec', 'donor'), ifields.samp, ifields.samp_req,
    #                 ifields.samp_opt)
    if not (donor and spec and samp):
        raise RuntimeError('Beware: one or more core files not written.')
    return


start_time = time.clock()
if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Transform input TSVs into ICGC portal ready format.")
    p.add_argument('-i', '--input', action='store', dest='dir',
                   help='Directory of TSVs to transform. Required.')
    p.add_argument('-o', '--output', action='store', dest='out',
                   help='Created TSVs output directory. Defaults to input directory')
    p.add_argument('-d', '--database', action='store', dest='db', default=':memory:',
                   help='Option to store sqlite database of input data to file.')
    p.add_argument('-p', '--project', action='store', dest='project', default='PROJECT',
                   help='Project name.')
    p = p.parse_args()

    if not p.out:
        p.out = p.dir.strip('/')

    with sqlite3.connect(p.db) as conn:
        typedict = {}
        for table in listdir(p.dir):
            for filetype in ifields.file_aliases:
                for filealias in filetype:
                    if filealias in table:
                        load(conn, table, p.dir.strip('/'))
                        if filealias in typedict:
                            typedict[filetype[0]] = merge_tables(filetype[0] + '_in',
                                                                 [typedict[filetype[0]], re.sub('\..*$', '', table)])
                        else:
                            typedict[filetype[0]] = re.sub('\..*$', '', table)
        get_three_main(conn, p.out, p.project, typedict)

print time.clock() - start_time, 'seconds'
