select
    station.station_id,
    station.site_id,
    qw_sample.sample_start_dt,
    qw_sample.nwis_host,
    qw_sample.qw_db_no,
    qw_sample.record_no,
    nwis_wqx_medium_cd.wqx_act_med_nm,
    station.organization,
    station.site_type,
    station.huc,
    station.governmental_unit_code,
    station.geom,
    station.organization_name,
    qw_sample.sample_id,
    qw_sample.samp_type_cd,
    nwis_wqx_medium_cd.wqx_act_med_sub,
    qw_sample.sample_start_sg,
    qw_sample.sample_start_time_datum_cd,
    qw_sample.sample_end_sg,
    qw_sample.sample_end_dt,
    sample_parameter.V00003,
    sample_parameter.V00098,
    sample_parameter.V50280,
    sample_parameter.V71999,
    sample_parameter.V72015,
    sample_parameter.V72016,
    sample_parameter.V78890,
    sample_parameter.V78891,
    sample_parameter.V82047,
    sample_parameter.V82048,
    sample_parameter.V82398,
    sample_parameter.V82398_fxd_tx,
    sample_parameter.v84164_fxd_tx,
    nawqa_sites.site_no nawqa_site_no,
    proto_org.proto_org_nm,
    qw_sample.coll_ent_cd,
    qw_sample.sample_lab_cm_tx,
    aqfr.aqfr_nm,
    hyd_cond_cd.hyd_cond_nm,
    hyd_event_cd.hyd_event_nm
from qw_sample
         join sitefile on qw_sample.site_id = sitefile.site_id
         join station_swap_nwis station on sitefile.site_id = station.station_id
         left join tu on cast(qw_sample.tu_id as integer) = tu.tu_id
         left join nwis_wqx_medium_cd on qw_sample.medium_cd = nwis_wqx_medium_cd.nwis_medium_cd
         left join body_part on cast(qw_sample.body_part_id as integer) = body_part.body_part_id
         left join proto_org on qw_sample.coll_ent_cd = proto_org.proto_org_cd
         left join hyd_event_cd on qw_sample.hyd_event_cd = hyd_event_cd.hyd_event_cd
         left join hyd_cond_cd on qw_sample.hyd_cond_cd = hyd_cond_cd.hyd_cond_cd
         join nwis_district_cds_by_host
              on sitefile.district_cd = nwis_district_cds_by_host.district_cd and
                 sitefile.nwis_host = nwis_district_cds_by_host.host_name
         left join aqfr
                   on qw_sample.aqfr_cd = aqfr.aqfr_cd and
                      sitefile.state_cd = aqfr.state_cd
         left join sample_parameter on qw_sample.sample_id = sample_parameter.sample_id
         left join nawqa_sites
                   on sitefile.site_no = nawqa_sites.site_no and
                      sitefile.agency_cd = nawqa_sites.agency_cd
where qw_sample.sample_web_cd = 'Y' and
        qw_sample.qw_db_no = '01' and
        sitefile.dec_lat_va <> 0 and
        sitefile.dec_long_va <> 0 and
        sitefile.db_no = '01' and
        sitefile.site_web_cd = 'Y' and
        sitefile.site_tp_cd not in ('FA-WTP', 'FA-WWTP', 'FA-TEP', 'FA-HP');