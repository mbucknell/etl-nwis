where qw_sample.sample_web_cd = 'Y' and
       qw_sample.qw_db_no = '01' and
       sitefile.dec_lat_va <> 0 and
       sitefile.dec_long_va <> 0 and
       sitefile.db_no = '01' and
       sitefile.site_web_cd = 'Y' and
       sitefile.site_tp_cd not in ('FA-WTP', 'FA-WWTP', 'FA-TEP', 'FA-HP')