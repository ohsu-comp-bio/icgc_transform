from os import listdir
import pandas
import sqlite3
import argparse
import re
import icgc_fields as ifields
import generic_fields as gfields


def get_key(conn, table):
    h = headers(conn, table)
    sql = 'select count(*) from {}'.format(table)
    for i in conn.execute(sql):
        total_rows = i[0]
    for header in h:
        sql = 'select count(distinct {}) from {}'.format(header, table)
        for j in conn.execute(sql):
            if j[0] == total_rows:
                return header,
    for i in range(len(h)):
        j = i + 1
        while j < len(h):
            sql = 'select count(*) from (select distinct {}, {} from {})'.format(h[i], h[j], table)
            for count in conn.execute(sql):
                if count[0] == total_rows:
                    return h[i], h[j]
            j += 1


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
    for ord in order:
        if ord in typedict:
            table = typedict[ord]
            h = headers(conn, table)
            for i in range(len(h)):
                if h[i] in ifields.aliases:
                    k = ifields.aliases[h[i]]
                    if k in all_fields and k not in grabbed_headers:
                        grabbed_headers.append(k)
                        fields.append(table + '.' + h[i] + ' as ' + k)
                        if table not in grab_tables:
                            grab_tables.append(table)
    for req_f in req:
        if req_f not in grabbed_headers and req_f == ifields.unique[filetype]:
            for table in grab_tables:
                key = get_key(conn, table)
                if len(key) == 1:
                    fields.append(key[0] + ' as ' + req_f)
                elif len(key) > 1:
                    fields.append(' || '.join('ifnull(' + table + '.' + s + ', "")' for s in key) + ' as ' + req_f)
    fields = ", ".join(fields)
    while len(grab_tables) > 1:
        # Note: This while is untested. It is meant to deal with the issues of pulling data from more than one table.
        grab_tables[0] = merge_tables('merge', [grab_tables[0], grab_tables[1]], drop=False)
        grab_tables.remove(grab_tables[1])
    return "select distinct {} from {}".format(fields, grab_tables[0])


def process_outfile(file, all_fields, fields_req, fields_opt):
    for i in range(len(all_fields)):
        if all_fields[i] not in file.columns.values.tolist():
            file.insert(i, all_fields[i], value='', allow_duplicates=False)
    w2 = file.reindex(columns=all_fields)
    for i in all_fields:
        if i in fields_req:
            if i == 'specimen_type':
                w2[i] = w2[i].replace(to_replace=['', None], value='UNKNOWN')
            else:
                w2 = w2[pandas.notnull(w2[i])]
        elif i not in fields_opt:
            w2[i] = w2[i].replace(to_replace=['', None], value='888')
    return w2


def get_main(conn, filename, dir, typedict, order, all_fields, req, opt):
    for filetype in order:
        if filetype in typedict.keys():
            sql = make_query(conn, order, filetype, typedict, all_fields, req)
            break
    if not sql:
        return False
    out = pandas.read_sql(sql, conn)
    w2 = process_outfile(out, all_fields, req, opt)
    w2.to_csv(dir + '/ICGC' + filename.lower() + '.tsv', sep='\t', index=False)
    print filename + ' file written.'
    return True


def get_three_main(conn, dir, typedict):
    donor = get_main(conn, 'Donor', dir, typedict, ('donor', 'spec', 'samp'), ifields.donor, ifields.donor_req,
                     ifields.donor_opt)
    spec = get_main(conn, 'Specimen', dir, typedict, ('spec', 'samp', 'donor'), ifields.spec, ifields.spec_req,
                    ifields.spec_opt)
    samp = get_main(conn, 'Sample', dir, typedict, ('samp', 'spec', 'donor'), ifields.samp, ifields.samp_req,
                    ifields.samp_opt)
    if not (donor and spec and samp):
        raise RuntimeError('Beware: one or more core files not written.')
    return


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Transform input TSVs into ICGC portal ready format.")
    p.add_argument('-i', '--input', action='store', dest='dir',
                   help='Directory of TSVs to transform. Required.')
    p.add_argument('-o', '--output', action='store', dest='out',
                   help='Created TSVs output directory. Defaults to input directory')
    p.add_argument('-d', '--database', action='store', dest='db', default=':memory:',
                   help='Option to store sqlite database of input data to file.')
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
                            typedict[filetype[0]] = merge_tables(filetype[0], [typedict[filetype[0]], re.sub('\..*$', '', table)])
                        else:
                            typedict[filetype[0]] = re.sub('\..*$', '', table)
        get_three_main(conn, p.out, typedict)
