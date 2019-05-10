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