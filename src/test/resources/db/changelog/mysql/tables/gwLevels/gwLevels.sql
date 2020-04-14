CREATE TABLE GW_LEVELS (
  site_id int(11),
  lev_str_dt char(19),
  lev_dtm datetime,
  lev_dt_acy_cd char(1),
  lev_tz_cd char(6),
  lev_tz_offset char(6),
  lev_va char(7),
  lev_acy_cd char(1),
  parameter_cd char(5),
  lev_datum_cd char(9),
  lev_src_cd char(1),
  lev_status_cd char(1),
  lev_meth_cd char(1),
  lev_agency_cd char(5),
  lev_age_cd char(1),
  gw_levels_md timestamp
)
