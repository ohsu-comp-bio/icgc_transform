aliases = {
    'individualId' : 'donor_id',
    'gender' : 'donor_sex',
    'description' : 'donor_sex',
    'vital_status' : 'donor_vital_status',
    'age_at_diagnosis' : 'donor_age_at_diagnosis',
    'diagnosis' : 'donor_diagnosis_icd10',
    'patient_comment' : 'donor_notes',
#    'cgd_specimen_id' : 'specimen_id',
    'organ' : 'specimen_type',
    'specimen_type' : 'specimen_type',
    'processing' : 'specimen_processing',
    'cell_viability' : 'percentage_cellularity',
    'cell_concentration' : 'level_of_cellularity',
    'sampleId' : 'specimen_id'
}

o_aliases = {
    'donor_id' : ['individualId'],
    'donor_sex' : ['gender', 'description'],
    'donor_region_of_residence' : [],
    'donor_vital_status' : ['vital_status'],
    'disease_status_last_followup' : [],
    'donor_relapse_type' : [],
    'donor_age_at_diagnosis' : ['age_at_diagnosis'],
    'donor_age_at_enrollment' : [],
    'donor_age_at_last_followup' : [],
    'donor_relapse_interval' : [],
    'donor_diagnosis_icd10' : ['diagnosis'],
    'donor_tumour_staging_system_at_diagnosis' : [],
    'donor_tumour_stage_at_diagnosis' : [],
    'donor_tumour_stage_at_diagnosis_supplemental' : [],
    'donor_survival_time' : [],
    'donor_interval_of_last_followup' : [],
    'donor_notes' : ['patient_comment'],
    'prior_malignancy' : [],
    'cancer_type_prior_malignancy' : [],
    'cancer_history_first_degree_relative' : [],
    'donor_primary_treatment_interval' : [],
    "specimen_id" : ['cgd_specimen_id'],
    "specimen_type" : ['specimen_type', 'organ'],
    "specimen_type_other" : [],
    "specimen_interval" : [],
    "specimen_donor_treatment_type" : [],
    "specimen_donor_treatment_type_other" : [],
    "specimen_processing" : ['processing'],
    "specimen_processing_other" : [],
    "specimen_storage" : [],
    "specimen_storage_other" : [],
    "tumour_confirmed"  : [],
    "specimen_biobank" : [],
    "specimen_biobank_id" : [],
    "specimen_available" : [],
    "tumour_histological_type" : [],
    "tumour_grading_system" : [],
    "tumour_grade" : [],
    "tumour_grade_supplemental" : [],
    "tumour_stage_system" : [],
    "tumour_stage" : [],
    "tumour_stage_supplemental" : [],
    "digital_image_of_stained_section" : [],
    "percentage_cellularity" : ['cell_viability'],
    "level_of_cellularity" : [],
    "specimen_notes" : [],
    "analyzed_sample_id" : ['sampleId'],
    "analyzed_sample_interval" : [],
    "analyzed_sample_notes" : [],
    "study" : []
}

file_aliases = [
    ['donor', 'patient', 'individual'],
    ['samp'],
    ['spec', 'specimen'],
    ['copy_num'],
    ['gen_var']
]

unique = {
    'donor' : 'donor_id',
    'spec' : 'specimen_id',
    'samp' : 'analyzed_sample_id'
}

donor = [
    'donor_id',
    'donor_sex',
    'donor_region_of_residence',
    'donor_vital_status',
    'disease_status_last_followup',
    'donor_relapse_type',
    'donor_age_at_diagnosis',
    'donor_age_at_enrollment',
    'donor_age_at_last_followup',
    'donor_relapse_interval',
    'donor_diagnosis_icd10',
    'donor_tumour_staging_system_at_diagnosis',
    'donor_tumour_stage_at_diagnosis',
    'donor_tumour_stage_at_diagnosis_supplemental',
    'donor_survival_time',
    'donor_interval_of_last_followup',
    'donor_notes',
    'prior_malignancy',
    'cancer_type_prior_malignancy',
    'cancer_history_first_degree_relative',
    'donor_primary_treatment_interval'
]

donor_req = [
    "donor_id"
]

donor_opt = [
    "donor_notes"
]

spec = [
    "donor_id",
    "specimen_id",
    "specimen_type",
    "specimen_type_other",
    "specimen_interval",
    "specimen_donor_treatment_type",
    "specimen_donor_treatment_type_other",
    "specimen_processing",
    "specimen_processing_other",
    "specimen_storage",
    "specimen_storage_other",
    "tumour_confirmed",
    "specimen_biobank",
    "specimen_biobank_id",
    "specimen_available",
    "tumour_histological_type",
    "tumour_grading_system",
    "tumour_grade",
    "tumour_grade_supplemental",
    "tumour_stage_system",
    "tumour_stage",
    "tumour_stage_supplemental",
    "digital_image_of_stained_section",
    "percentage_cellularity",
    "level_of_cellularity",
    "specimen_notes"
]

spec_req = [
    "donor_id",
    "specimen_id",
    "specimen_type"
]

spec_opt = [
    "specimen_notes"
]

samp = [
    "analyzed_sample_id",
    "specimen_id",
    "analyzed_sample_interval",
    "percentage_cellularity",
    "level_of_cellularity",
    "analyzed_sample_notes",
    "study"
]

samp_req = [
    "analyzed_sample_id",
    "specimen_id"
]

samp_opt = [
    "analyzed_sample_interval",
    "analyzed_sample_notes",
    "study"
]

vars = {
    'donor' : [donor, donor_req, donor_opt],
    'spec' : [spec, spec_req, spec_opt],
    'samp' : [samp, samp_req, samp_opt]
}
