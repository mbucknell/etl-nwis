load data
badfile 'QW_RESULT.bad'
TRUNCATE INTO TABLE NWIS_WS_STAR.qw_result
fields terminated by "\t"
(
   sample_id   nullif sample_id = 'NULL',
   site_id  nullif site_id = 'NULL',
   record_no   nullif record_no = 'NULL',
   result_web_cd  nullif result_web_cd = 'NULL',
   parameter_cd     nullif parameter_cd = 'NULL',
   meth_cd  nullif meth_cd = 'NULL',
   result_va nullif result_va = 'NULL',
   result_unrnd_va nullif result_unrnd_va = 'NULL',
   result_rd  nullif result_rd = 'NULL',
   rpt_lev_va  nullif rpt_lev_va = 'NULL',
   rpt_lev_cd  nullif rpt_lev_cd = 'NULL',
   lab_std_va  nullif lab_std_va = 'NULL',
   remark_cd   nullif remark_cd = 'NULL',
   val_qual_tx  nullif val_qual_tx = 'NULL',
   null_val_qual_cd  nullif null_val_qual_cd = 'NULL',
   qa_cd  nullif qa_cd = 'NULL',
   dqi_cd   nullif dqi_cd = 'NULL',
   anl_ent_cd  nullif anl_ent_cd = 'NULL',
   anl_set_no  nullif anl_set_no = 'NULL',
   anl_dt  nullif anl_dt = 'NULL',
   prep_set_no   nullif prep_set_no = 'NULL',
   prep_dt  nullif prep_dt = 'NULL',
   result_field_cm_tx  char(300) nullif result_field_cm_tx = 'NULL',
   result_lab_cm_tx  char(300) nullif result_lab_cm_tx = 'NULL',
   result_md  date "YYYY-MM-DD HH24:MI:SS" nullif result_md = '0000-00-00 00:00:00',
   qw_result_md  date "YYYY-MM-DD HH24:MI:SS" nullif qw_result_md = '0000-00-00 00:00:00'
)
