import pandas


def donor_sex(headers, frame):
    if 'donor_sex' in headers:
        frame['donor_sex'] = frame['donor_sex'].replace(to_replace=['M', 'Male'], value='1')
        frame['donor_sex'] = frame['donor_sex'].replace(to_replace=['F', 'Female'], value='2')
        frame['donor_sex'] = frame['donor_sex'].replace(to_replace=['', None], value='888')
    return frame

def donor_vital_status(headers, frame):
    if 'donor_vital_status' in headers:
        frame['donor_vital_status'] = frame['donor_vital_status'].replace(to_replace=['Deceased'], value='2')
        frame['donor_vital_status'] = frame['donor_vital_status'].replace(to_replace=['Alive'], value='1')
        frame['donor_vital_status'] = frame['donor_vital_status'].replace(to_replace=['Unknown', 'UNKNOWN'], value='unknown')
        frame['donor_vital_status'] = frame['donor_vital_status'].replace(to_replace=['', None], value='888')
    return frame

def disease_status_last_followup(headers, frame):
    if 'disease_status_last_followup' in headers:
        '1 complete remission'
        '2 partial remission'
        '3 progression'
        '4 relapse'
        '5 stable'
        '6 no evidence of disease'

def donor_relapse_type(headers, frame):
    if 'donor_relapse_type' in headers:
        '1 local recurrence'
        '2 distant recurrence/metastasis'
        '3 progession (liquid tumours)'
        '4 local recurrence and distant metastasis'

def prior_malignancy(headers, frame):
    if 'prior_malignancy' in headers:
        '1 yes'
        '2 no'
        '3 unknown'

def cancer_history_first_degree_relative(headers, frame):
    if 'cancer_history_first_degree_relative' in headers:
        '1 yes'
        '2 no'
        '3 unknown'



def specimen_type(headers, frame):
    if 'specimen_type' in headers:
        frame['specimen_type'] = frame['specimen_type'].replace(to_replace=['', None], value='UNKNOWN')
        # '101 Normal - solid tissue'
        # '102 Normal - blood derived'
        # '103 Normal - bone marrow'
        # '104 Normal - tissue adjacent to primary'
        # '105 Normal - buccal cell'
        # '106 Normal - EBV immortalized'
        # '107 Normal - lymph node'
        # '108 Normal - other'
        # '109 Primary tumour - solid tissue'
        # '110 Primary tumour - blood derived (peripheral blood)'
        # '111 Primary tumour - blood derived (bone marrow)'
        # '112 Primary tumour - additional new primary'
        # '113 Primary tumour - other'
        # '114 Recurrent tumour - solid tissue'
        # '115 Recurrent tumour - blood derived (peripheral blood)'
        # '116 Recurrent tumour - blood derived (bone marrow)'
        # '117 Recurrent tumour - other'
        # '118 Metastatic tumour - NOS'
        # '119 Metastatic tumour - lymph node'
        # '120 Metastatic tumour - metastasis local to lymph node'
        # '121 Metastatic tumour - metastasis to distant location'
        # '122 Metastatic tumour - additional metastatic'
        # '123 Xenograft - derived from primary tumour'
        # '124 Xenograft - derived from tumour cell line'
        # '125 Cell line - derived from tumour'
        # '126 Primary tumour - lymph node'
        # '127 Metastatic tumour - other'
        # '128 Cell line - derived from xenograft tumour'
    return frame

def specimen_donor_treatment_type(headers, frame):
    if 'specimen_donor_treatment_type' in headers:
        frame['specimen_donor_treatment_type'] = frame['specimen_donor_treatment_type'].replace(to_replace=['', None], value='UNKNOWN')
    '1 no treatment'
    '2 chemotherapy'
    '3 radiation therapy'
    '4 combined chemo+radiation therapy'
    '5 immunotherapy'
    '6 combined chemo+immunotherapy'
    '7 surgery'
    '8 other therapy'
    '9 bone marrow transplant'
    '10 stem cell transplant'
    '11 monoclonal antibodies (for liquid tumours)'
    return frame

def specimen_processing(headers, frame):
    '1 cryopreservation in liquid nitrogen (dead tissue)'
    '2 cryopreservation in dry ice (dead tissue)'
    '3 cryopreservation of live cells in liquid nitrogen'
    '4 cryopreservation, other'
    '5 formalin fixed, unbuffered'
    '6 formalin fixed, buffered'
    '7 formalin fixed & paraffin embedded'
    '8 fresh'
    '9 other technique'

def specimen_storage(headers, frame):
    '1 frozen, liquid nitrogen'
    '2 frozen, -70 freezer'
    '3 frozen, vapor phase'
    '4 RNA later frozen'
    '5 paraffin block'
    '6 cut slide'
    '7 other'

def tumour_confirmed(headers, frame):
    '1 yes'
    '2 no'

def specimen_available(headers, frame):
    '1 yes'
    '2 no'

def level_of_cellularity(headers, frame):
    '1 1-20%'
    '2 21-40%'
    '3 41-60%'
    '4 61-80%'
    '5 >81%'

def study(headers, frame):
    '1 PCAWG'