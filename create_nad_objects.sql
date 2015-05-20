show user;
select * from global_name;

create or replace package create_nad_objects as
	procedure main(mesg in out varchar2, success_notify in varchar2, failure_notify in varchar2);
	procedure create_pc_result;
	procedure create_station;
    procedure create_summaries;
	procedure create_code_tables;
	procedure create_station_indexes;
	procedure create_result_indexes;
	procedure create_station_sum_indexes;
	procedure create_result_sum_indexes;
	procedure create_result_ct_sum_indexes;
	procedure create_result_nr_sum_indexes;
	procedure analyze_tables;
	procedure validate;
	procedure install;
end create_nad_objects;
/


create or replace package body create_nad_objects
      /*-----------------------------------------------------------------------------------
        package create_nad_objects                                created by barry, 05/2011

        This package is run after four staging tables (SITEFILE, QW_SAMPLE, QW_RESULT, and
        SERIES_CATALOG) are unloaded from the nwisweb mysql database and into temp tables
        in wiws.  That activity occurs on server igsarm-cida-mysql1.

        FA_STATION - about 1.5 million rows, based on most rows from SITEFILE
           PK_ISN (formerly SITE_ID) is the key to fa_station.
           It would appear that the derived column STATION_ID (agency_cd || ''-'' || site_no),
           which is referenced on web pages as SITE_ID, is supposed to be a natural key,
           but it doesn't quite work. (about 100 dups)  Would need to add district_cd, but
           that one is not carried through to FA_STATION.

        FA_REGULAR_RESULT - over 80 million rows, based on most rows from QW_RESULT.

        key in QW_RESULT is (sample_id, parameter_cd)
        One key in FA_REGULAR_RESULT is (sample_id  , parameter_code),
                        an alternate is (activity_id, parameter_code)
                           [activity_id is the former QW_SAMPLE's nwis_host/qw_db_no/record_no ]
                           [so, sample_id is an artificial key for nwis_host/qw_db_no/record_no]
        FA_REGULAR_RESULT references FA_STATION.PK_ISN via FK_STATION. [from QW_RESULT's SITE_ID]

        QW_SAMPLE has key sample_id.  Each sample_id has multple QW_RESULT rows,
        each with a different parm_cd
        -----------------------------------------------------------------------------------*/
   as
   lf constant varchar(1) := chr(10);
   	table_suffix constant user_tables.table_name%type := 'nwis';

   message varchar2(600);
--   suffix varchar2(10);

--   type cleanuptable is table of varchar2(80) index by binary_integer;
--   cleanup cleanuptable;
--
--   table_list varchar2(4000 char) := 'translate(table_name, ''0123456789'', ''0000000000'') in ' ||
--                                     '(''FA_REGULAR_RESULT_00000'',''FA_STATION_00000'',''SERIES_CATALOG_00000'',''QWPORTAL_SUMMARY_00000'',' ||
--                                      '''NWIS_STATION_SUM_00000'',''NWIS_RESULT_SUM_00000'',''NWIS_RESULT_CT_SUM_00000'',''NWIS_RESULT_NR_SUM_00000'',' ||
--                                      '''NWIS_LCTN_LOC_00000'',''PUBLIC_SRSNAMES_00000'',''NWIS_DI_ORG_00000'',''CHARACTERISTICNAME_00000'',' ||
--                                      '''CHARACTERISTICTYPE_00000'',''COUNTRY_00000'',''COUNTY_00000'',''ORGANIZATION_00000'',''SAMPLEMEDIA_00000'',' ||
--                                      '''SITETYPE_00000'',''STATE_00000'')';
                                      
--   type cursor_type is ref cursor;

--   procedure determine_suffix
--   is
--      drop_remnants cursor_type;
--      query         varchar2(4000) := 'select table_name from user_tables where ' || table_list ||
--                                      ' and substr(table_name, -5) = substr(:current_suffix, 2) order by case when table_name like ''FA_STATION%'' then 2 else 1 end, table_name';
--
--      drop_name varchar2(30);
--      stmt      varchar2(80);
--
--   begin
--
--      select '_' || to_char(nvl(max(to_number(substr(table_name, length('FA_REGULAR_RESULT_') + 1)) + 1), 1), 'fm00000')
--        into suffix from user_tables
--        where translate(table_name, '0123456789', '0000000000') = 'FA_REGULAR_RESULT_00000';
--
--      dbms_output.put_line('using ''' || suffix || ''' for suffix.');
--
--      open drop_remnants for query using suffix;
--      loop
--         fetch drop_remnants into drop_name;
--         exit when drop_remnants%NOTFOUND;
--         stmt := 'drop table ' || drop_name || ' cascade constraints purge';
--         dbms_output.put_line('CLEANUP remnants: ' || stmt);
--         execute immediate stmt;
--      end loop;
--
--   exception
--      when others then
--         message := 'FAIL to determine suffix: ' || SQLERRM;
--         dbms_output.put_line(message);
--   end determine_suffix;

	procedure create_pc_result is
    begin

        dbms_output.put_line('dropping nwis pc_result indexes...');
        etl_helper.drop_indexes('pc_result_swap_nwis');
        
        dbms_output.put_line('populating nwis pc_result...');
        execute immediate 'truncate table pc_result_swap_nwis';
        
        insert /*+ append nologging parallel */
          into pc_result_swap_nwis (wqp_id, data_source_id, data_source, station_id, site_id, event_date, analytical_method, p_code, activity,
                                    characteristic_name, characteristic_type, sample_media, organization, site_type, huc_12, governmental_unit_code,
                                    geom, organization_name, activity_type_code, activity_media_subdiv_name, activity_start_time,
                                    act_start_time_zone, activity_stop_date, activity_stop_time, act_stop_time_zone, activity_depth,
                                    activity_depth_unit, activity_depth_ref_point, activity_upper_depth, activity_upper_depth_unit,
                                    activity_lower_depth, activity_lower_depth_unit, activity_uprlwr_depth_ref_pt, project_id,
                                    activity_conducting_org, activity_comment, sample_aqfr_name, hydrologic_condition_name, hydrologic_event_name,
                                    sample_collect_method_id, sample_collect_method_ctx, sample_collect_method_name, sample_collect_equip_name,
                                    result_id, result_detection_condition_tx, sample_fraction_type, result_measure_value, result_unit,
                                    result_meas_qual_code, result_value_status, statistic_type, result_value_type, weight_basis_type, duration_basis,
                                    temperature_basis_level, particle_size, precision, result_comment, result_depth_meas_value,
                                    result_depth_meas_unit_code, result_depth_alt_ref_pt_txt, sample_tissue_taxonomic_name,
                                    sample_tissue_anatomy_name, analytical_procedure_id, analytical_procedure_source, analytical_method_name,
                                    analytical_method_citation, lab_name, analysis_date_time, lab_remark, myql, myqlunits, myqldesc,
                                    analysis_prep_date_tx)
        select rownum wqp_id,
               2 data_source_id,
               cast('NWIS' as varchar2(4000 char)) data_source,
               s.station_id,
               s.site_id,
               trunc(samp.sample_start_dt) event_date,
               nemi.nemi_url analytical_method,
               r.parameter_cd p_code,
               samp.nwis_host || '.' || samp.qw_db_no || '.' || samp.record_no activity,
               parm.srsname characteristic_name,
               parm.parm_seq_grp_nm characteristic_type,
               wqx_medium_cd.wqx_act_med_nm sample_media,
               s.organization,
               s.site_type,
               s.huc_12,
               s.governmental_unit_code,
               s.geom,
               s.organization_name,
               case 
                 when samp.samp_type_cd = 'A' then 'Not determined'
                 when samp.samp_type_cd = 'B' then 'Quality Control Sample-Other'
                 when samp.samp_type_cd = 'H' then 'Sample-Composite Without Parents'
                 when samp.samp_type_cd = '1' then 'Quality Control Sample-Field Spike'
                 when samp.samp_type_cd = '2' then 'Quality Control Sample-Field Blank'
                 when samp.samp_type_cd = '3' then 'Quality Control Sample-Reference Sample'
                 when samp.samp_type_cd = '4' then 'Quality Control Sample-Blind'
                 when samp.samp_type_cd = '5' then 'Quality Control Sample-Field Replicate'
                 when samp.samp_type_cd = '6' then 'Quality Control Sample-Reference Material'
                 when samp.samp_type_cd = '7' then 'Quality Control Sample-Field Replicate'
                 when samp.samp_type_cd = '8' then 'Quality Control Sample-Spike Solution'
                 when samp.samp_type_cd = '9' then 'Sample-Routine'
                 else 'Unknown'
               end activity_type_code,
               wqx_medium_cd.wqx_act_med_sub activity_media_subdiv_name,
               case 
                 when samp.sample_start_sg in ('m', 'h') then to_char(samp.sample_start_dt, 'hh24:mi:ss')
                 else null
               end activity_start_time,
               case
                 when samp.sample_start_dt is not null and samp.sample_start_sg in ('h','m') then samp.sample_start_time_datum_cd
                 else null
               end act_start_time_zone,
               case
                 when samp.sample_end_sg in ('m', 'h', 'D') then substr(samp.sample_end_dt, 1, 10)
                 when samp.sample_end_sg = 'M' then substr(samp.sample_end_dt, 1, 7)
                 when samp.sample_end_sg = 'Y' then substr(samp.sample_end_dt, 1, 4)
                 else null
               end activity_stop_date,
               case when samp.sample_end_sg in ('m', 'h') then substr(samp.sample_end_dt,12) else null end activity_stop_time,
               case 
                 when samp.sample_end_dt is not null and samp.sample_end_sg in ('h', 'm') then samp.sample_start_time_datum_cd
                 else null
               end act_stop_time_zone,
               coalesce(parameter.V00003, parameter.V00098, parameter.V78890, parameter.V78891) activity_depth,
               case
                 when parameter.V00003 is not null then 'feet'
                 when parameter.V00098 is not null then 'meters'
                 when parameter.V78890 is not null then 'feet'
                 when parameter.V78891 is not null then 'meters'
                 else null
               end activity_depth_unit,
               case
                 when parameter.V00003 is not null or parameter.V00098 is not null then null
                 when parameter.V78890 is not null or parameter.V78891 is not null then 'Below mean sea level'
                 else null
               end activity_depth_ref_point,
               coalesce(parameter.V72015, parameter.V82047) activity_upper_depth,
               nvl2(coalesce(parameter.V72015, parameter.V82047),
                    case
                      when parameter.V72015 is not null then 'feet'
                      when parameter.V82047 is not null then 'meters'
                      when parameter.V72016 is not null then 'feet'
                      when parameter.V82048 is not null then 'meters'
                      else null
                    end,
                    null) activity_upper_depth_unit,
               case
                 when parameter.V72015 is not null then parameter.V72016
                 when parameter.V82047 is not null then parameter.V82048
                 when parameter.V72016 is not null then parameter.V72016
                 when parameter.V82048 is not null then parameter.V82048
                 else null
               end activity_lower_depth,
               nvl2(case
                      when parameter.V72015 is not null then parameter.V72016
                      when parameter.V82047 is not null then parameter.V82048
                      when parameter.V72016 is not null then parameter.V72016
                      when parameter.V82048 is not null then parameter.V82048
                      else null
                    end,
                    case
                      when parameter.V72015 is not null then 'feet'
                      when parameter.V82047 is not null then 'meters'
                      when parameter.V72016 is not null then 'feet'
                      when parameter.V82048 is not null then 'meters'
                      else null
                    end,
                    null) activity_lower_depth_unit,
               case when parameter.V72015 is not null then 'Below land-surface datum'
                 when parameter.V82047 is not null then ''
                 when parameter.V72016 is not null then 'Below land-surface datum'
                 when parameter.V82048 is not null then ''
                 else null
               end activity_uprlwr_depth_ref_pt,
               coalesce(parameter.v71999_fxd_nm, samp.project_cd, 'USGS') project_id,
               coalesce(proto_org2.proto_org_nm, samp.coll_ent_cd) activity_conducting_org,
               trim(samp.sample_lab_cm_tx) activity_comment,
               aqfr.sample_aqfr_name,
               hyd_cond_cd.hyd_cond_nm hydrologic_condition_name,
               hyd_event_cd.hyd_event_nm hydrologic_event_name,
               case
                 when parameter.v84164_fxd_tx is not null and parameter.v82398_fxd_tx is not null
                   then parameter.V82398
                 else 'USGS'
               end sample_collect_method_id,
               case
                 when parameter.v84164_fxd_tx is not null and parameter.v82398_fxd_tx is not null
                   then cast('USGS parameter code 82398' as varchar2(25))
                 else 'USGS'
               end sample_collect_method_ctx,
               case
                 when parameter.v84164_fxd_tx is not null and parameter.v82398_fxd_tx is not null
                   then parameter.v82398_fxd_tx
                 else 'USGS'
               end sample_collect_method_name,
               case
                 when parameter.v84164_fxd_tx is not null and parameter.v82398_fxd_tx is not null
                   then parameter.v84164_fxd_tx
                 else 'Unknown'
               end sample_collect_equip_name,
               rownum result_id,
               case
                 when r.remark_cd = 'U' then 'Not Detected'
                 when r.remark_cd = 'V' then 'Systematic Contamination'
                 when r.remark_cd = 'S' then null
                 when r.remark_cd = 'M' then 'Detected Not Quantified'
                 when r.remark_cd = 'N' then 'Detected Not Quantified'
                 when r.remark_cd = 'A' then null
                 when r.remark_cd = '<' then 'Not Detected'
                 when r.remark_cd = '>' then 'Present Above Quantification Limit'
                 else null
               end result_detection_condition_tx,
               parm.parm_frac_tx sample_fraction_type,
               nvl(fxd.fxd_tx,
                   case
                     when r.remark_cd in ('U', 'M', 'N', '<', '>') then null
                     when r.remark_cd in ('R', 'V', 'S', 'E', 'A') or r.remark_cd is null then r.result_va
                     else r.result_va
                   end) result_measure_value,
               nvl2(nvl(fxd.fxd_tx,
                        case
                          when r.remark_cd in ('U', 'M', 'N', '<', '>') then null
                          when r.remark_cd in ('R', 'V', 'S', 'E', 'A') or r.remark_cd is null then r.result_va
                          else r.result_va
                        end),
                    parm.parm_unt_tx, null) result_unit,
               null result_meas_qual_code,
               case
                 when r.dqi_cd = 'S' then 'Preliminary'
                 when r.dqi_cd = 'A' then 'Historical'
                 when r.dqi_cd = 'R' then 'Accepted'
                 else null
               end result_value_status,
               nvl(parm.parm_stat_tx,
                    case
                      when r.remark_cd = 'S' then 'MPN'
                      when r.remark_cd = 'A' then 'mean'
                      else null
                    end) statistic_type,
               case
                 when r.result_md is null then 'Calculated'
                 when r.remark_cd = 'E' then 'Estimated'
                 else 'Actual'
               end result_value_type,
               parm.parm_wt_tx weight_basis_type,
               parm.parm_tm_tx duration_basis,
               parm.parm_temp_tx temperature_basis_level,
               trim(parm.parm_size_tx) particle_size,
               r.lab_std_va precision,
               trim(r.result_lab_cm_tx) result_comment,
               null result_depth_meas_value,
               null result_depth_meas_unit_code,
               null result_depth_alt_ref_pt_txt,
               case
                 when parm.PARM_MEDIUM_TX = 'Biological Tissue' then tu.composite_tu_name
               end sample_tissue_taxonomic_name,
               case
                 when parm.parm_medium_tx = 'Biological Tissue' then body_part.body_part_nm
                 else null
               end sample_tissue_anatomy_name,
               r.meth_cd analytical_procedure_id,
               case
                 when r.meth_cd is not null then 'USGS'
                 else null
               end analytical_procedure_source,
               meth.meth_nm analytical_method_name,
               meth.cit_nm analytical_method_citation,
               proto_org.proto_org_nm lab_name,
               case 
                 when r.anl_dt is not null
                   then substr(r.anl_dt, 1, 4) || '-' || substr(r.anl_dt, 5, 2) || '-' || substr(r.anl_dt, 7, 2)
                 else null
               end analysis_date_time,
               trim(val_qual_cd1.val_qual_nm ||
                    val_qual_cd2.val_qual_nm ||
                    val_qual_cd3.val_qual_nm ||
                    val_qual_cd4.val_qual_nm ||
                    val_qual_cd5.val_qual_nm ||
                    case
                      when r.remark_cd = 'R' then 'Result below sample specific critical level.'
                      else null
                    end) lab_remark,
               case
                 when r.remark_cd = '<' and r.rpt_lev_va is null then r.result_va
                 when r.remark_cd = '<' and to_number(r.result_unrnd_va) > to_number(r.rpt_lev_va) then r.result_va
                 when r.remark_cd = '>' then r.result_va
                 when r.remark_cd in ('N', 'U') and r.rpt_lev_va is not null then r.rpt_lev_va
                 when r.remark_cd = 'M' and r.rpt_lev_va is null and r.result_unrnd_va is null then null
                 when r.remark_cd = 'M' and r.rpt_lev_va is null and r.result_unrnd_va is not null then nvl(z_parm_meth.multiplier, parm.multiplier)
                 when nwis_wqx_rpt_lev_cd.rpt_lev_cd is not null then r.rpt_lev_va
                 else null
               end myql,
               nvl2(case
                      when r.remark_cd = '<' and r.rpt_lev_va is null then r.result_va
                      when r.remark_cd = '<' and to_number(r.result_unrnd_va) > to_number(r.rpt_lev_va) then r.result_va
                      when r.remark_cd = '>' then r.result_va
                      when r.remark_cd in ('N', 'U') and r.rpt_lev_va is not null then r.rpt_lev_va
                      when r.remark_cd = 'M' and r.rpt_lev_va is null and r.result_unrnd_va is null then null
                      when r.remark_cd = 'M' and r.rpt_lev_va is null and r.result_unrnd_va is not null then nvl(z_parm_meth.multiplier, parm.multiplier)
                      when nwis_wqx_rpt_lev_cd.rpt_lev_cd is not null then r.rpt_lev_va
                      else null
                    end,
                    parm.parm_unt_tx,
                    null) myqlunits,
               nvl2(case
                      when r.remark_cd = '<' and r.rpt_lev_va is null then r.result_va
                      when r.remark_cd = '<' and to_number(r.result_unrnd_va) > to_number(r.rpt_lev_va) then r.result_va
                      when r.remark_cd = '>' then r.result_va
                      when r.remark_cd in ('N', 'U') and r.rpt_lev_va is not null then r.rpt_lev_va
                      when r.remark_cd = 'M' and r.rpt_lev_va is null and r.result_unrnd_va is null then null
                      when r.remark_cd = 'M' and r.rpt_lev_va is null and r.result_unrnd_va is not null then nvl(z_parm_meth.multiplier, parm.multiplier)
                      when nwis_wqx_rpt_lev_cd.rpt_lev_cd is not null then r.rpt_lev_va
                      else null
                    end,
                    case
                      when r.remark_cd = '<' and r.rpt_lev_va is null then 'Historical Lower Reporting Limit'
                      when r.remark_cd = '<' and to_number(r.result_unrnd_va) > to_number(r.rpt_lev_va) then 'Elevated Detection Limit'
                      when r.remark_cd = '<' and nwis_wqx_rpt_lev_cd.rpt_lev_cd is not null then nwis_wqx_rpt_lev_cd.wqx_rpt_lev_nm
                      when r.remark_cd = '>' then 'Upper Reporting Limit'
                      when r.remark_cd = 'M' and r.rpt_lev_va is null and r.result_unrnd_va is null then null
                      when r.remark_cd = 'M' and r.rpt_lev_va is null and r.result_unrnd_va is not null then 'Lower Quantitation Limit'
                      when r.remark_cd = 'M' and r.rpt_lev_va is not null then nwis_wqx_rpt_lev_cd.wqx_rpt_lev_nm
                      when r.rpt_lev_va is not null then nwis_wqx_rpt_lev_cd.wqx_rpt_lev_nm
                      else null
                    end,
                    null) myqldesc,
               case
                 when r.prep_dt is not null then substr(r.prep_dt, 1, 4) || '-' || substr(r.prep_dt, 5, 2) || '-' || substr(r.prep_dt, 7, 2)
                 else null
               end analysis_prep_date_tx 
          from nwis_qw_result r,
               nwis_qw_sample samp,
               (select tz_cd, tz_utc_offset_tm
                  from nwq_stg_lu_tz
                 where tz_cd is not null
                union 
                select tz_dst_cd, tz_dst_utc_offset_tm tz_utc_offset_tm
                  from nwq_stg_lu_tz
                where tz_dst_cd is not null) lu_tz,
                nwis_sitefile site,
                station_swap_nwis s,
                (select /*+ full(p2) parallel(p2, 4) full(fxd_71999) parallel(fxd_71999, 4) full(fxd_82398) parallel(fxd_82398, 4)
                            full(fxd_84164) parallel(fxd_84164, 4)
                            use_hash(p2) use_hash(fxd_71999) use_hash(82398) use_hash(84164) */
                        sample_id,
                        v71999,
                        v50280,
                        v72015,
                        v82047,
                        v72016,
                        v82048,
                        v00003,
                        v00098,
                        v78890,
                        v78891,
                        v82398,
                        v84164,
                        v71999_fxd_nm,
                        v82398_fxd_tx,
                        v84164_fxd_tx
                   from (select /*+ full(p1) parallel(p1, 4) */
                                sample_id,
                                max(case when parameter_cd = '71999' then result_unrnd_va else null end) AS V71999,
                                max(case when parameter_cd = '50280' then result_unrnd_va else null end) AS V50280,
                                max(case when parameter_cd = '72015' then result_unrnd_va else null end) AS V72015,
                                max(case when parameter_cd = '82047' then result_unrnd_va else null end) AS V82047,
                                max(case when parameter_cd = '72016' then result_unrnd_va else null end) AS V72016,
                                max(case when parameter_cd = '82048' then result_unrnd_va else null end) AS V82048,
                                max(case when parameter_cd = '00003' then result_unrnd_va else null end) AS V00003,
                                max(case when parameter_cd = '00098' then result_unrnd_va else null end) AS V00098,
                                max(case when parameter_cd = '78890' then result_unrnd_va else null end) AS V78890,
                                max(case when parameter_cd = '78891' then result_unrnd_va else null end) AS V78891,
                                max(case when parameter_cd = '82398' then result_unrnd_va else null end) AS V82398,
                                max(case when parameter_cd = '84164' then result_unrnd_va else null end) AS V84164
                           from nwis_qw_result p1
                          where result_web_cd = 'Y' and
                                parameter_cd in ('71999', '50280', '72015', '82047', '72016', '82048', '00003', '00098', '78890', '78891', '82398', '84164')
                             group by sample_id) p2,
                         (select fxd_nm v71999_fxd_nm, fxd_va from nwis_ws_stg_fxd where parm_cd = '71999') fxd_71999,
                         (select fxd_tx v82398_fxd_tx, fxd_va from nwis_ws_stg_fxd where parm_cd = '82398') fxd_82398,
                         (select fxd_tx v84164_fxd_tx, fxd_va from nwis_ws_stg_fxd where parm_cd = '84164') fxd_84164
                  where p2.v71999 = fxd_71999.fxd_va(+) and
                        p2.v82398 = fxd_82398.fxd_va(+) and
                        p2.v84164 = fxd_84164.fxd_va(+)) parameter,
                (select tu_id,
                        trim(tu_1_nm) ||
                          case when trim(tu_2_cd) is not null then ' ' || trim(tu_2_cd) end ||
                          case when trim(tu_2_nm) is not null then ' ' || trim(tu_2_nm) end ||
                          case when trim(tu_3_cd) is not null then ' ' || trim(tu_3_cd) end ||
                          case when trim(tu_3_nm) is not null then ' ' || trim(tu_3_nm) end ||
                          case when trim(tu_4_cd) is not null then ' ' || trim(tu_4_cd) end ||
                          case when trim(tu_4_nm) is not null then ' ' || trim(tu_4_nm) end AS composite_tu_name
                   from nwis_ws_stg_tu) tu,
                (select trim(wqx_act_med_nm) wqx_act_med_nm,
                        trim(wqx_act_med_sub) wqx_act_med_sub,
                        trim(nwis_medium_cd) medium_cd
                   from nwis_wqx_medium_cd) wqx_medium_cd,
                (select trim(body_part_nm) body_part_nm,
                        trim(body_part_id) body_part_id
                   from nwis_ws_stg_body_part) body_part,
                (select /*+ full(a) full(b) full(z_parm_meth2) use_hash(a) use_hash(b) use_hash(z_parm_meth2) */
                        a.parm_unt_tx,
                        a.parm_frac_tx,
                        a.parm_medium_tx,
                        a.parm_stat_tx,
                        a.parm_wt_tx,
                        a.parm_temp_tx,
                        a.parm_tm_tx,
                        a.parm_cd,
                        a.parm_size_tx,
                        b.parm_seq_grp_nm,
                        z_parm_alias.srsname,
                        z_parm_alias.srsid,
                        z_parm_alias.casrn,
                        z_parm_meth2.multiplier
                   from nwq_stg_lu_parm a,
                        nwq_stg_lu_parm_seq_grp_cd b,
                        (select parm_cd,
                                max(case when parm_alias_cd = 'SRSNAME' then parm_alias_nm else null end) AS srsname,
                                max(case when parm_alias_cd = 'SRSID'   then parm_alias_nm else null end) AS srsid  ,
                                max(case when parm_alias_cd = 'CASRN'   then parm_alias_nm else null end) AS casrn
                           from nwq_stg_lu_parm_alias
                              group by parm_cd
                              having max(case when parm_alias_cd = 'SRSNAME' then parm_alias_nm else null end) is not null) z_parm_alias,
                        (select decode(REGEXP_INSTR(PARM_METH_RND_TX, '[1-9]', 1, 1),
                                       1, '0.001',
                                       2, '0.01',
                                       3, '0.1',
                                       4, '1.',
                                       5, '10',
                                       6, '100',
                                       7, '1000',
                                       8, '10000',
                                       9, '100000') multiplier,
                                parm_cd
                           from nwq_stg_lu_parm_meth
                          where meth_cd is null) z_parm_meth2
                  where a.parm_public_fg = 'Y' and
                        a.parm_seq_grp_cd = b.parm_seq_grp_cd(+) and
                        a.parm_cd = z_parm_alias.parm_cd and
                        a.parm_cd = z_parm_meth2.parm_cd(+)) parm,
                (select fxd_tx,
                        parm_cd,
                        fxd_va
                   from nwis_ws_stg_fxd) fxd,
                (select proto_org_nm,
                        proto_org_cd
                   from nwis_ws_stg_proto_org) proto_org,
                (select proto_org_nm,
                        proto_org_cd
                   from nwis_ws_stg_proto_org) proto_org2,
                (select /*+ full(meth1) full(z_cit_meth) use_hash(meth1) use_hash(z_cit_meth) */
                        meth1.meth_cd,
                        meth1.meth_nm,
                        z_cit_meth.cit_nm
                   from nwis_ws_stg_meth meth1,
                        (select meth_cd, min(cit_nm) cit_nm from nwis_ws_stg_z_cit_meth group by meth_cd) z_cit_meth
                  where meth1.meth_cd = z_cit_meth.meth_cd(+)) meth,
                (select decode(REGEXP_INSTR(PARM_METH_RND_TX, '[1-9]', 1, 1),
                               1, '0.001',
                               2, '0.01',
                               3, '0.1',
                               4, '1.',
                               5, '10',
                               6, '100',
                               7, '1000',
                               8, '10000',
                               9, '100000') multiplier,
                        parm_cd,
                        meth_cd
                   from nwq_stg_lu_parm_meth) z_parm_meth,
                (select rpt_lev_cd, wqx_rpt_lev_nm from nwis_wqx_rpt_lev_cd) nwis_wqx_rpt_lev_cd,
                (select val_qual_nm || '. ' val_qual_nm, val_qual_cd from nwis_ws_stg_val_qual_cd) val_qual_cd1,
                (select val_qual_nm || '. ' val_qual_nm, val_qual_cd from nwis_ws_stg_val_qual_cd) val_qual_cd2,
                (select val_qual_nm || '. ' val_qual_nm, val_qual_cd from nwis_ws_stg_val_qual_cd) val_qual_cd3,
                (select val_qual_nm || '. ' val_qual_nm, val_qual_cd from nwis_ws_stg_val_qual_cd) val_qual_cd4,
                (select val_qual_nm || '. ' val_qual_nm, val_qual_cd from nwis_ws_stg_val_qual_cd) val_qual_cd5,
                (select trim(hyd_event_cd) hyd_event_cd, trim(hyd_event_nm) hyd_event_nm from nwis_ws_stg_hyd_event_cd) hyd_event_cd,
                (select trim(hyd_cond_cd) hyd_cond_cd, trim(hyd_cond_nm) hyd_cond_nm from nwis_ws_stg_hyd_cond_cd) hyd_cond_cd,
                (select district_cd, host_name from nwis_district_cds_by_host) dist,
                (select aqfr_cd, state_cd, trim(aqfr_nm) as SAMPLE_AQFR_NAME from nwis_ws_stg_aqfr) aqfr,
                (select analytical_procedure_source,
                        analytical_procedure_id,
                        case
                          when method_id is not null
                            then
                              case method_type
                                when 'analytical'
                                  then 'https://www.nemi.gov/methods/method_summary/' || method_id || '/'
                                when 'statistical'
                                  then 'https://www.nemi.gov/methods/sams_method_summary/' || method_id || '/'
                              end
                           else 
                             null 
                        end nemi_url
                   from wqp_nemi_nwis_crosswalk) nemi
         where
            r.result_web_cd    = 'Y'                         and
           (r.RESULT_VA        is not null  OR
            r.RPT_LEV_VA       is not null  OR
            r.REMARK_CD        is not null)                  and
            r.sample_id        = samp.sample_id              and
            r.parameter_cd     = parm.parm_cd              and  /* not outer join on z_parm or z_parm_alias */
            r.parameter_cd     = fxd.parm_cd(+)              and
            case when r.result_va = '0.0' then '0' else r.result_va end = fxd.fxd_va(+) and
            r.anl_ent_cd       = proto_org.proto_org_cd(+)   and
            r.meth_cd          = meth.meth_cd(+)             and
            r.parameter_cd     = z_parm_meth.parm_cd(+)      and
            r.meth_cd          = z_parm_meth.meth_cd(+)      and
            r.rpt_lev_cd       = nwis_wqx_rpt_lev_cd.rpt_lev_cd(+) and
            substr(r.val_qual_tx, 1, 1) = val_qual_cd1.val_qual_cd(+) and
            substr(r.val_qual_tx, 2, 1) = val_qual_cd2.val_qual_cd(+) and
            substr(r.val_qual_tx, 3, 1) = val_qual_cd3.val_qual_cd(+) and
            substr(r.val_qual_tx, 4, 1) = val_qual_cd4.val_qual_cd(+) and
            substr(r.val_qual_tx, 5, 1) = val_qual_cd5.val_qual_cd(+) and
            samp.sample_web_cd = 'Y'                         and
            samp.qw_db_no      = '01'                        and
            samp.site_id       = site.site_id                  and
            samp.sample_id     = parameter.sample_id(+)        and
            to_number(samp.tu_id) = tu.tu_id(+)                and
            samp.medium_cd     = wqx_medium_cd.medium_cd(+)    and
            samp.body_part_id  = body_part.body_part_id(+)     and
            samp.coll_ent_cd   = proto_org2.proto_org_cd(+)    and
            samp.hyd_event_cd  = hyd_event_cd.hyd_event_cd(+)  and
            samp.hyd_cond_cd   = hyd_cond_cd.hyd_cond_cd(+)    and
            site.dec_lat_va    <> 0                            and
            site.dec_long_va   <> 0                            and
            site.db_no         = '01'                        and
            site.site_web_cd   = 'Y'                         and
            site.site_tp_cd not in ('FA-WTP', 'FA-WWTP', 'FA-TEP', 'FA-HP')  and
            site.nwis_host  not in ('fltlhsr001', 'fltpasr001', 'flalssr003') and
            site.district_cd   = dist.district_cd              and
            site.nwis_host     = dist.host_name and
            samp.SAMPLE_START_TIME_DATUM_CD = lu_tz.tz_cd(+) and
            samp.aqfr_cd  = aqfr.aqfr_cd (+) and
            site.state_cd = aqfr.state_cd(+) and
            nvl2(r.meth_cd, 'USGS', null) = nemi.analytical_procedure_source(+) and
            trim(r.meth_cd) = nemi.analytical_procedure_id(+) and
            site.site_id = s.station_id;
        commit;

    exception
        when others then
            message := 'FAIL to create FA_REGULAR_RESULT: ' || SQLERRM;
            dbms_output.put_line(message);
    end create_pc_result;

    procedure create_station is
       begin

        dbms_output.put_line('dropping nwis station indexes...');
        etl_helper.drop_indexes('station_swap_nwis');
        
        dbms_output.put_line('populating nwis station...');
        execute immediate 'truncate table station_swap_nwis';
        
        --This does not need to be an execute immediate...
        execute immediate
             q'!insert /*+ append nologging parallel */
                 into station_swap_nwis (data_source_id, data_source, station_id, site_id, organization, site_type, huc_12, governmental_unit_code,
                                         geom, station_name, organization_name, description_text, latitude, longitude, map_scale,
                                         geopositioning_method, hdatum_id_code, elevation_value, elevation_unit, elevation_method, vdatum_id_code,
                                         coordinates, drain_area_value, drain_area_unit, contrib_drain_area_value, contrib_drain_area_unit,
                                         geoposition_accy_value, geoposition_accy_unit, vertical_accuracy_value, vertical_accuracy_unit,
                                         nat_aqfr_name, aqfr_name, aqfr_type_name, construction_date, well_depth_value, well_depth_unit,
                                         hole_depth_value, hole_depth_unit)
               select 2 data_source_id,
                      'NWIS' data_source,
                      sitefile.site_id station_id,
                      sitefile.agency_cd || '-' || sitefile.site_no site_id,
                      ndcbh.organization_id organization,
                      site_tp.primary_site_type site_type,
                      case when length(sitefile.huc_cd) = 8 then sitefile.huc_cd else null end huc_12,
                      sitefile.country_cd || ':' || sitefile.state_cd || ':' || sitefile.county_cd governmental_unit_code,
                      mdsys.sdo_geometry(2001,8265,mdsys.sdo_point_type(round(sitefile.dec_long_va, 7),round(sitefile.dec_lat_va, 7), null), null, null) geom,
                      trim(sitefile.station_nm) station_name,
                      ndcbh.organization_name,
                      trim(sitefile.site_rmks_tx) description_text,
                      round(sitefile.dec_lat_va , 7) latitude,
                      round(sitefile.dec_long_va, 7) longitude,
                      trim(sitefile.map_scale_fc) map_scale,
                      nvl(geo_meth.geopositioning_method, 'Unknown') geopositioning_method,
                      nvl(sitefile.dec_coord_datum_cd, 'Unknown') hdatum_id_code,
                      case when sitefile.alt_datum_cd is not null then case when sitefile.alt_va = '.' then '0' else trim(sitefile.alt_va) end else null end elevation_value,
                      case when sitefile.alt_va is not null and sitefile.alt_datum_cd is not null then 'feet' else null end elevation_unit,
                      vert.vertical_method_name elevation_method,
                      case when sitefile.alt_va is not null then sitefile.alt_datum_cd else null end vdatum_id_code,
                      round(sitefile.dec_long_va , 7) || ',' || round(sitefile.dec_lat_va, 7) coordinates,
                      to_number(sitefile.drain_area_va) drain_area_value,
                      nvl2(sitefile.drain_area_va, 'sq mi', null) drain_area_unit,
                      case when sitefile.contrib_drain_area_va = '.' then 0 else to_number(sitefile.contrib_drain_area_va) end contrib_drain_area_value,
                      nvl2(sitefile.contrib_drain_area_va, 'sq mi', null) contrib_drain_area_unit,
                      geo_accuracy.geopositioning_accuracy_value geoposition_accy_value,
                      geo_accuracy.geopositioning_accuracy_units geoposition_accy_unit,
                      case when sitefile.alt_va is not null and sitefile.alt_datum_cd is not null then trim(sitefile.alt_acy_va) else null end vertical_accuracy_value,
                      case when sitefile.alt_va is not null and sitefile.alt_datum_cd is not null and sitefile.alt_acy_va is not null then 'feet' else null end vertical_accuracy_unit,
                      nat_aqfr.nat_aqfr_name,
                      aqfr.aqfr_name,
                      aqfr_type.aqfr_type_name,
                      sitefile.construction_dt construction_date,
                      to_number(case when sitefile.well_depth_va in ('.', '-') then '0' else sitefile.well_depth_va end) well_depth_value,
                      nvl2(sitefile.well_depth_va, 'ft', null) well_depth_unit,
                      to_number(case when sitefile.hole_depth_va in ('.', '-') then '0' else sitefile.hole_depth_va end) hole_depth_value,
                      nvl2(sitefile.hole_depth_va, 'ft', null) hole_depth_unit
                 from nwis_sitefile sitefile
                      join (select cast('USGS-' || state_postal_cd as varchar2(7)) organization_id,
                                   'USGS ' || STATE_NAME || ' Water Science Center' organization_name,
                                   host_name,
                                   district_cd
                              from nwis_district_cds_by_host) ndcbh
                        on sitefile.nwis_host = ndcbh.host_name and    /* host name must exist - no outer join */
                           sitefile.district_cd  = ndcbh.district_cd
                      left join (select cast(state_post_cd as varchar2(2)) as state_postal_cd, state_cd, country_cd from nwis_ws_stg_stage_state) postal
                        on sitefile.country_cd = postal.country_cd and
                           sitefile.state_cd = postal.state_cd
                      left join (select a.site_tp_cd,
                                        case when a.site_tp_prim_fg = 'Y' then a.site_tp_ln
                                          else b.site_tp_ln || ': ' || a.site_tp_ln
                                        end as station_type_name,
                                        case when a.site_tp_prim_fg = 'Y' then a.site_tp_ln
                                          else b.site_tp_ln
                                        end as primary_site_type
                                   from nwis_ws_stg_site_tp a,
                                        nwis_ws_stg_site_tp b
                                  where substr(a.site_tp_cd, 1, 2) = b.site_tp_cd and
                                        b.site_tp_prim_fg = 'Y') site_tp
                        on sitefile.site_tp_cd = site_tp.site_tp_cd
                      left join (select nwis_name as vertical_method_name , nwis_code from nwis_ws_stg_misc_lookups where category = 'Altitude Method') vert
                        on sitefile.alt_meth_cd = vert.nwis_code
                      left join (select nwis_name as geopositioning_method, nwis_code from nwis_ws_stg_misc_lookups where category = 'Lat/Long Method') geo_meth
                        on sitefile.coord_meth_cd= geo_meth.nwis_code
                      left join (select inferred_value as geopositioning_accuracy_value,
                                        inferred_units as geopositioning_accuracy_units,
                                        nwis_code
                                   from nwis_ws_stg_misc_lookups
                                  where category = 'Lat-Long Coordinate Accuracy') geo_accuracy
                        on sitefile.coord_acy_cd = geo_accuracy.nwis_code
                      left join (select nat_aqfr_nm as nat_aqfr_name, nat_aqfr_cd from nwis_ws_stg_nat_aqfr group by nat_aqfr_nm, nat_aqfr_cd) nat_aqfr
                        on sitefile.nat_aqfr_cd  = nat_aqfr.nat_aqfr_cd
                      left join (select nwis_name as aqfr_type_name, nwis_code from nwis_ws_stg_misc_lookups where CATEGORY='Aquifer Type Code') aqfr_type
                        on sitefile.aqfr_type_cd = aqfr_type.nwis_code
                      left join (select aqfr_nm as aqfr_name, trim(state_cd) state_cd, aqfr_cd from nwis_ws_stg_aqfr) aqfr
                        on sitefile.aqfr_cd = aqfr.aqfr_cd and
                           sitefile.state_cd     = aqfr.state_cd
                where sitefile.DEC_LAT_VA   <> 0   and
                      sitefile.DEC_LONG_VA  <> 0   and
                      sitefile.site_web_cd  = 'Y'  and
                      sitefile.db_no        = '01' and
                      sitefile.site_tp_cd not in ('FA-WTP', 'FA-WWTP', 'FA-TEP', 'FA-HP')   and
                      sitefile.nwis_host  not in ('fltlhsr001', 'fltpasr001', 'flalssr003')!';
                     

    exception
        when others then
            message := 'FAIL to create STATION_SWAP_NWIS: ' || SQLERRM;
            dbms_output.put_line(message);
    end create_station;


    procedure create_summaries is
       begin

        dbms_output.put_line('dropping nwis station_sum indexes...');
        etl_helper.drop_indexes('station_sum_swap_nwis');
        
        dbms_output.put_line('populating nwis station_sum...');
        execute immediate 'truncate table station_sum_swap_nwis';

        insert /*+ append nologging parallel */
        into station_sum_swap_nwis (data_source_id, data_source, station_id, site_id, organization, site_type, huc_12, governmental_unit_code,
                                    geom, result_count)
        select station.data_source_id,
               station.data_source,
               station.station_id,
               station.site_id,
               station.organization,
               station.site_type,
               station.huc_12,
               station.governmental_unit_code,
               station.geom,
               nvl(pc_result.pc_result_count,0),
               0
          from station_swap_nwis station
               left join (select station_id, count(*) pc_result_count
                            from pc_result_swap_nwis
                               group by station_id) pc_result
                 on station.station_id = pc_result.station_id;
        commit;

                    
        dbms_output.put_line('dropping nwis pc_result_sum indexes...');
        etl_helper.drop_indexes('pc_result_sum_swap_nwis');
        
        dbms_output.put_line('populating nwis pc_result_sum...');
        execute immediate 'truncate table pc_result_sum_swap_nwis';

		insert /*+ append nologging parallel */
		  into pc_result_sum_swap_nwis (data_source_id, data_source, station_id, site_id, event_date, analytical_method, p_code, activity,
                                        characteristic_name, characteristic_type, sample_media, organization, site_type, huc_12,
                                        governmental_unit_code, pc_result_count)
        select /*+ full(a) parallel(a, 4) full(b) parallel(b, 4) use_hash(a) use_hash(b) */
               a.data_source_id,
               a.data_source,
               a.station_id,
               a.site_id,
               b.event_date,
               b.analytical_method,
               b.p_code,
               b.activity,
               b.characteristic_name,
               b.characteristic_type,
               b.sample_media,
               a.organization,
               a.site_type,
               a.huc_12,
               a.governmental_unit_code,
               b.result_count
          from station_sum_swap_nwis a
               left join (select station_id, sample_media, characteristic_type, characteristic_name, p_code,
                                 event_date, analytical_method, activity,
                                 nvl(count(*), 0) result_count
                            from pc_result_swap_nwis
                               group by station_id, sample_media, characteristic_type, characteristic_name, p_code,
                                        event_date, analytical_method, activity
                         ) b
                 on a.station_id = b.station_id;
        commit;

        dbms_output.put_line('dropping nwis pc_result_ct_sum indexes...');
        etl_helper.drop_indexes('pc_result_ct_sum_swap_nwis');
        
        dbms_output.put_line('populating nwis pc_result_ct_sum...');
        execute immediate 'truncate table pc_result_ct_sum_swap_nwis';

          insert /*+ append nologging parallel */
          into pc_result_ct_sum_swap_nwis (data_source_id, data_source, station_id, site_id, governmental_unit_code, site_type, organization,
                                           huc_12, sample_media, characteristic_type, characteristic_name, analytical_method,
                                           p_code, pc_result_count)
        select /*+ full(b) parallel(b, 4) */
               data_source_id,
               data_source,
               station_id,
               site_id,
               governmental_unit_code,
               site_type,
               organization,
               huc_12,
               sample_media,
               characteristic_type,
               characteristic_name,
               analytical_method,
               p_code,
               sum(pc_result_count) pc_result_count
          from pc_result_sum_swap_nwis
             group by data_source_id,
                      data_source,
                      site_id,
                      station_id,
                      governmental_unit_code,
                      site_type,
                      organization,
                      huc_12,
                      sample_media,
                      characteristic_type,
                      characteristic_name,
                      analytical_method,
                      p_code;
        commit;


        dbms_output.put_line('dropping nwis pc_result_nr_sum indexes...');
        etl_helper.drop_indexes('pc_result_nr_sum_swap_nwis');
        
        dbms_output.put_line('populating nwis pc_result_nr_sum...');
        execute immediate 'truncate table pc_result_nr_sum_swap_nwis';

          insert /*+ append nologging parallel */
          into pc_result_nr_sum_swap_nwis (data_source_id, data_source, station_id, event_date, analytical_method, p_code,
                                           activity, characteristic_name, characteristic_type, sample_media, pc_result_count)
        select data_source_id, data_source, station_id, event_date, analytical_method, p_code,
               activity, characteristic_name, characteristic_type, sample_media,
               sum(pc_result_count) pc_result_count
          from pc_result_sum_swap_nwis
             group by data_source_id, data_source, station_id, event_date, analytical_method, p_code,
               activity, characteristic_name, characteristic_type, sample_media;
        commit;

--      dbms_output.put_line('creating nwis_lctn_loc...');
--
--      execute immediate
--     'create table nwis_lctn_loc' || suffix || ' compress pctfree 0 nologging noparallel as
--      select /*+ parallel(4) */ distinct
--             country_cd,
--             state_cd state_fips,
--             organization_id,
--             organization_name
--        from fa_station' || suffix;
--
--      cleanup(7) := 'drop table nwis_lctn_loc' || suffix || ' cascade constraints purge';
--      
--      dbms_output.put_line('creating nwis_di_org...');
--
--      execute immediate
--     'create table nwis_di_org' || suffix || ' compress pctfree 0 nologging noparallel as
--      select distinct
--             cast(''USGS-'' || state_postal_cd as varchar2(7)) as organization_id,
--             ''USGS '' || STATE_NAME || '' Water Science Center'' as organization_name
--        from nwis_district_cds_by_host';
--
--      cleanup(8) := 'drop table nwis_di_org' || suffix || ' cascade constraints purge';

    exception
        when others then
            message := 'FAIL to create summaries: ' || SQLERRM;
            dbms_output.put_line(message);
    end create_summaries;

--   procedure create_series_catalog
--   is
--   begin
--
--      dbms_output.put_line('creating series catalog...');
--
--      execute immediate
--     'create table SERIES_CATALOG' || suffix || ' compress pctfree 0 nologging as
--      select
--         c.site_id       AS fk_station,
--         c.data_type_cd  AS data_type_code,
--         c.parm_cd       AS parameter_code,
--         c.stat_cd       AS status_code,
--         c.loc_nm        AS location_name,
--         c.medium_grp_cd AS medium_group_code,
--         c.parm_grp_cd   AS parameter_group_code,
--         c.srs_id,
--         c.access_cd     AS access_code,
--         c.begin_date    AS begin_date_text,
--         c.end_date      AS end_date_text,
--         c.count_nu,
--         c.series_catalog_md
--      from
--         temp_series_catalog c,
--         fa_station' || suffix || ' s,
--         lu_parm p
--      where
--         c.site_id = s.pk_isn and
--         c.parm_cd = p.parm_cd';
--
--      cleanup(9) := 'drop table SERIES_CATALOG' || suffix || ' cascade constraints purge';
--
--   exception
--      when others then
--         message := 'FAIL to create SERIES_CATALOG: ' || SQLERRM;
--         dbms_output.put_line(message);
--   end create_series_catalog;
--
--   procedure create_public_srsnames
--   is
--   begin
--
--      dbms_output.put_line('creating public_srsnames...');
--
--      execute immediate
--     'create table public_srsnames' || suffix || ' compress pctfree 0 nologging as
--      select lu_parm.parm_cd,
--             lu_parm.parm_ds description,
--             lu_parm_alias.parm_alias_nm characteristicname,
--             lu_parm.parm_unt_tx measureunitcode,
--             lu_parm.parm_frac_tx resultsamplefraction,
--             lu_parm.parm_temp_tx resulttemperaturebasis,
--             lu_parm.parm_stat_tx resultstatisticalbasis,
--             lu_parm.parm_tm_tx resulttimebasis,
--             lu_parm.parm_wt_tx resultweightbasis,
--             lu_parm.parm_size_tx resultparticlesizebasis,
--             case
--               when lu_parm.parm_rev_dt > lu_parm_alias.parm_alias_rev_dt
--                 then lu_parm.parm_rev_dt
--               else lu_parm_alias.parm_alias_rev_dt
--             end last_rev_dt,
--             max(case
--                   when lu_parm.parm_rev_dt > lu_parm_alias.parm_alias_rev_dt
--                     then lu_parm.parm_rev_dt
--                   else lu_parm_alias.parm_alias_rev_dt
--                 end) over () max_last_rev_dt
--        from lu_parm
--             join lu_parm_alias
--               on lu_parm.parm_cd = lu_parm_alias.parm_cd and
--                  ''SRSNAME'' = lu_parm_alias.parm_alias_cd
--       where parm_public_fg = ''Y''
--         order by 1';
--
--      cleanup(10) := 'drop table public_srsnames' || suffix || ' cascade constraints purge';
--
--   exception
--      when others then
--         message := 'FAIL to create public_srsnames: ' || SQLERRM;
--         dbms_output.put_line(message);
--   end create_public_srsnames;

    procedure create_code_tables is
    begin

		dbms_output.put_line('populating nwis characteristic_name...');
        execute immediate 'truncate table characteristic_name_swap_nwis';

        insert /*+ append nologging parallel */
          into characteristic_name_swap_nwis (data_source_id, code_value)
        select distinct data_source_id,
                        characteristic_name code_value
          from pc_result_swap_nwis
         where characteristic_name is not null;
        commit;
        

        
          dbms_output.put_line('populating nwis characteristic_type...');
        execute immediate 'truncate table characteristic_type_swap_nwis';

        insert /*+ append nologging parallel */
          into characteristic_type_swap_nwis (data_source_id, code_value)
        select distinct data_source_id,
                        characteristic_type code_value
          from pc_result_swap_nwis
         where characteristic_type is not null;
        commit;


        
        dbms_output.put_line('populating nwis country...');
        execute immediate 'truncate table country_swap_nwis';

        insert /*+ append nologging parallel */
          into country_swap_nwis (data_source_id, code_value, description)
        select distinct station_sum_swap_nwis.data_source_id,
                        station_sum_swap_nwis.country_code code_value,
                        nwis_ws_stg_stage_country.country_nm description
          from station_sum_swap_nwis
               join nwis_ws_stg_stage_country
                 on station_sum_swap_nwis.country_code = trim(nwis_ws_stg_stage_country.country_cd)
         where station_sum_swap_nwis.country_code is not null;
        commit;

                  

        dbms_output.put_line('populating nwis county...');
        execute immediate 'truncate table county_swap_nwis';

        insert /*+ append nologging parallel */
          into county_swap_nwis (data_source_id, code_value, description)
        select distinct station_sum_swap_nwis.data_source_id,
                        station_sum_swap_nwis.county_code code_value,
                        station_sum_swap_nwis.country_code || ', ' ||
                            nwis_ws_stg_stage_state.state_nm || ', ' ||
                            nwis_ws_stg_stage_county.county_nm description
          from station_sum_swap_nwis
               left join nwis_ws_stg_stage_state
                 on station_sum_swap_nwis.country_code = trim(nwis_ws_stg_stage_state.country_cd) and
                    regexp_substr(station_sum_swap_nwis.state_code, '[^:]+', 1, 2) = trim(nwis_ws_stg_stage_state.state_cd)
               left join nwis_ws_stg_stage_county
                 on station_sum_swap_nwis.country_code = trim(nwis_ws_stg_stage_county.country_cd) and
                    regexp_substr(station_sum_swap_nwis.state_code, '[^:]+', 1, 2) = trim(nwis_ws_stg_stage_county.state_cd) and
                    regexp_substr(station_sum_swap_nwis.county_code, '[^:]+', 1, 3) = trim(nwis_ws_stg_stage_county.county_cd)
         where station_sum_swap_nwis.county_code is not null;
        commit;
        
        

        dbms_output.put_line('populating nwis organization...');
        execute immediate 'truncate table organization_swap_nwis';
        
        insert /*+ append nologging parallel */
          into organization_swap_nwis (data_source_id, code_value, description)
        select distinct station_swap_nwis.data_source_id,
                        station_swap_nwis.organization code_value,
                        station_swap_nwis.organization_name description
          from station_swap_nwis
         where station_swap_nwis.organization is not null;
           commit;
          
           

           dbms_output.put_line('populating nwis sample_media...');
        execute immediate 'truncate table sample_media_swap_nwis';

        insert /*+ append nologging parallel */
          into sample_media_swap_nwis (data_source_id, code_value)
        select distinct data_source_id,
                        sample_media code_value
          from pc_result_swap_nwis
         where sample_media is not null;
        commit;
      
      

        dbms_output.put_line('populating nwis sitetype...');
        execute immediate 'truncate table site_type_swap_nwis';
        
        insert /*+ append nologging parallel */
          into site_type_swap_nwis (data_source_id, code_value)
        select distinct station_sum_swap_nwis.data_source_id,
                        station_sum_swap_nwis.site_type code_value
          from station_sum_swap_nwis
         where station_sum_swap_nwis.site_type is not null;
           commit;
           
           

          dbms_output.put_line('populating nwis state...');
        execute immediate 'truncate table state_swap_nwis';

        insert /*+ append nologging parallel */
          into state_swap_nwis (data_source_id, code_value, description_with_country, description_with_out_country)
        select distinct station_sum_swap_nwis.data_source_id,
                        station_sum_swap_nwis.state_code code_value,
                        station_sum_swap_nwis.country_code || ', ' ||
                            nwis_ws_stg_stage_state.state_nm description_with_country,
                         nwis_ws_stg_stage_state.state_nm description_with_out_country
          from station_sum_swap_nwis
               left join nwis_ws_stg_stage_state
                 on station_sum_swap_nwis.country_code = trim(nwis_ws_stg_stage_state.country_cd) and
                    regexp_substr(station_sum_swap_nwis.state_code, '[^:]+', 1, 2) = trim(nwis_ws_stg_stage_state.state_cd)
         where station_sum_swap_nwis.state_code is not null;
        commit;

    exception
          when others then
            message := 'FAIL to create code_tables: ' || SQLERRM;
            dbms_output.put_line(message);
    end create_code_tables;
   
    
--          stmt := 'create table qwportal_summary' || suffix || ' compress nologging pctfree 0 cache as
--               select
--                  trim(station.state_cd) as fips_state_code,
--                  trim(station.county_cd) as fips_county_code,
--                  trim(station.state_cd) || trim(station.county_cd) as fips_state_and_county,
--                  cast(''N'' as varchar2(1 char)) as nwis_or_epa,
--                  /* took out trim(result.characteristic_type) as characteristic_type, */
--                  cast(primary_site_type as varchar2(30)) as site_type,
--                  cast(trim(station.hydrologic_unit_code) as varchar2(8)) as huc8,
--                  cast(null as varchar2(12)) as huc12,
--                  min(case when activity_start_date_time between to_date(''01-JAN-1875'', ''DD-MON-YYYY'') and sysdate then activity_start_date_time else null end) as min_date,
--                  max(case when activity_start_date_time between to_date(''01-JAN-1875'', ''DD-MON-YYYY'') and sysdate then activity_start_date_time else null end) as max_date,
--                  cast(count(distinct case when months_between(sysdate, activity_start_date_time) between 0 and 12 then sample_id else null end) as number(7)) as samples_past_12_months,
--                  cast(count(distinct case when months_between(sysdate, activity_start_date_time) between 0 and 60 then sample_id else null end) as number(7)) as samples_past_60_months,
--                  cast(count(distinct result.activity_id) as number(7)) samples_all_time,
--                  cast(sum(case when months_between(sysdate, activity_start_date_time) between 0 and 12 then 1 else 0 end) as number(7)) as results_past_12_months,
--                  cast(sum(case when months_between(sysdate, activity_start_date_time) between 0 and 60 then 1 else 0 end) as number(7)) as results_past_60_months,
--                  cast(count(*) as number(7)) as results_all_time
--               from
--                  nwis_ws_star.fa_station' || suffix || ' station,
--                  nwis_ws_star.fa_regular_result' || suffix || ' result
--               where
--                  station.pk_isn = result.fk_station and
--                  station.state_cd between ''01'' and ''56'' and
--                  length(trim(station.state_cd)) = 2
--               group by
--                  trim(station.state_cd),
--                  trim(station.county_cd),
--                  trim(station.state_cd) || trim(station.county_cd),
--                  /*  took out trim(result.characteristic_type), */
--                  primary_site_type,
--                  trim(station.hydrologic_unit_code)
--               union all
--               select
--                  fips_state_code,
--                  fips_county_code,
--                  fips_state_code || fips_county_code fips_state_and_county,
--                  cast(''E'' as varchar2(1)) nwis_or_epa,
--                  site_type,
--                  huc8,
--                  huc12,
--                  min_date,
--                  max_date,
--                  samples_past_12_months,
--                  samples_past_60_months,
--                  samples_all_time,
--                  results_past_12_months,
--                  results_past_60_months,
--                  results_all_time
--               from
--                  storetmodern.storet_sum';
--
--      cleanup(19) := 'drop table qwportal_summary' || suffix || ' cascade constraints purge';
--
--      dbms_output.put_line('creating qwportal_summary...');
--      execute immediate stmt;
--
--      stmt := 'alter table ' || table_name || ' cache noparallel';
--      dbms_output.put_line(stmt);
--      execute immediate stmt;
--
--      stmt := 'alter table ' || table_name || ' add constraint pk_' || table_name || ' primary key (pk_isn) using index nologging';
--      dbms_output.put_line(stmt);
--      execute immediate stmt;
      
      
    procedure create_station_indexes is
    begin

        etl_helper.create_station_indexes(table_suffix);
        
    end create_station_indexes;

    
    procedure create_result_indexes is
    begin

        etl_helper.create_result_indexes(table_suffix);

    end create_result_indexes;

    
    procedure create_station_sum_indexes is
        stmt            varchar2(32000);
    begin

        etl_helper.create_station_sum_indexes(table_suffix);

    end create_station_sum_indexes;

    
    procedure create_result_sum_indexes is
        stmt            varchar2(32000);
    begin

        etl_helper.create_result_sum_indexes(table_suffix);

    end create_result_sum_indexes;

    
    procedure create_result_ct_sum_indexes is
        stmt            varchar2(32000);
    begin

        etl_helper.create_result_ct_sum_indexes(table_suffix);
        
    end create_result_ct_sum_indexes;

    
    procedure create_result_nr_sum_indexes is
        stmt            varchar2(32000);
    begin

        etl_helper.create_result_nr_sum_indexes(table_suffix);
        
    end create_result_nr_sum_indexes;

    
    procedure analyze_tables is
        stmt            varchar2(32000);
    begin
        dbms_output.put_line('analyze station...');  /* takes about xx minutes*/
        dbms_stats.gather_table_stats('WQP_CORE', 'STATION_SWAP_NWIS', null, 100, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);

        dbms_output.put_line('analyze pc_result...');  /* takes about xx minutes */
        dbms_stats.gather_table_stats('WQP_CORE', 'PC_RESULT_SWAP_NWIS', null,  10, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
        
--        dbms_output.put_line('analyze series_catalog...');  /* takes about xx minutes */
--        dbms_stats.gather_table_stats('WQP_CORE', 'SERIES_CATALOG', null, 100, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
        
--        dbms_output.put_line('analyze qwportal_summary...');  /* takes about xx minutes */
--        dbms_stats.gather_table_stats('WQP_CORE', 'QWPORTAL_SUMMARY', null, 100, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
        
        dbms_output.put_line('analyze station_sum...');  /* takes about xx minutes */
        dbms_stats.gather_table_stats('WQP_CORE', 'STATION_SUM_SWAP_NWIS', null, 100, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
        
        dbms_output.put_line('analyze pc_result_sum...');  /* takes about xx minutes */
        dbms_stats.gather_table_stats('WQP_CORE', 'PC_RESULT_SUM_SWAP_NWIS', null, 10, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
        
        dbms_output.put_line('analyze pc_result_ct_sum...');  /* takes about xx minutes */
        dbms_stats.gather_table_stats('WQP_CORE', 'PC_RESULT_CT_SUM_SWAP_NWIS', null, 10, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
        
        dbms_output.put_line('analyze pc_result_nr_sum...');  /* takes about xx minutes */
        dbms_stats.gather_table_stats('WQP_CORE', 'PC_RESULT_NR_SUM_SWAP_NWIS', null, 10, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
        
--        dbms_output.put_line('analyze nwis_lctn_loc...');  /* takes about xx minutes */
--        dbms_stats.gather_table_stats('WQP_CORE', 'NWIS_LCTN_LOC', null, 10, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
        
--        dbms_output.put_line('analyze nwis_di_org...');  /* takes about xx minutes */
--        dbms_stats.gather_table_stats('WQP_CORE', 'NWIS_DI_ORG', null, 10, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
        
--        dbms_output.put_line('analyze public_srsnames...');  /* takes about xx minutes */
--        dbms_stats.gather_table_stats('WQP_CORE', 'PUBLIC_SRSNAMES', null, 10, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);

        dbms_output.put_line('analyze characteristic_name...');  /* takes about xx minutes */
        dbms_stats.gather_table_stats('WQP_CORE', 'CHARACTERISTIC_NAME_SWAP_NWIS', null, 10, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
        
        dbms_output.put_line('analyze characteristic_type...');  /* takes about xx minutes */
        dbms_stats.gather_table_stats('WQP_CORE', 'CHARACTERISTIC_TYPE_SWAP_NWIS', null, 10, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
        
        dbms_output.put_line('analyze country...');  /* takes about xx minutes */
        dbms_stats.gather_table_stats('WQP_CORE', 'COUNTRY_SWAP_NWIS', null, 10, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
        
        dbms_output.put_line('analyze county...');  /* takes about xx minutes */
        dbms_stats.gather_table_stats('WQP_CORE', 'COUNTY_SWAP_NWIS', null, 10, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
        
        dbms_output.put_line('analyze organization...');  /* takes about xx minutes */
        dbms_stats.gather_table_stats('WQP_CORE', 'ORGANIZATION_SWAP_NWIS', null, 10, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
        
        dbms_output.put_line('analyze sample_media...');  /* takes about xx minutes */
        dbms_stats.gather_table_stats('WQP_CORE', 'SAMPLE_MEDIA_SWAP_NWIS', null, 10, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
        
        dbms_output.put_line('analyze site_type...');  /* takes about xx minutes */
        dbms_stats.gather_table_stats('WQP_CORE', 'SITE_TYPE_SWAP_NWIS', null, 10, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
        
        dbms_output.put_line('analyze state...');  /* takes about xx minutes */
        dbms_stats.gather_table_stats('WQP_CORE', 'STATE_SWAP_NWIS', null, 10, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
        
    exception
        when others then
            message := 'FAIL in analyze  --> ' || SQLERRM;
            dbms_output.put_line(message);
    end analyze_tables;

    procedure validate is
        old_rows     int;
        new_rows     int;
        index_count  int;
        type cursor_type is ref cursor;
        c            cursor_type;
        query        varchar2(4000);
        pass_fail    varchar2(15);
        situation    varchar2(200);
    begin

        dbms_output.put_line('validating...');

        dbms_output.put_line('... pc_result');
        select count(*) into old_rows from pc_result partition (pc_result_nwis);
        select count(*) into new_rows from pc_result_swap_nwis;
        if new_rows > 70000000 and new_rows > old_rows - 9000000 then
            pass_fail := 'PASS';
        else
            pass_fail := 'FAIL';
            $IF $$empty_db $THEN
                  pass_fail := 'PASS empty_db';
               $END
          end if;
          situation := pass_fail || ': table comparison for pc_result: was ' || trim(to_char(old_rows, '999,999,999')) || ', now ' || trim(to_char(new_rows, '999,999,999'));
          dbms_output.put_line(situation);
          if pass_fail = 'FAIL' and message is null then
            message := situation;
          end if;

        dbms_output.put_line('... station');
          select count(*) into old_rows from station partition (station_nwis);
          select count(*) into new_rows from station_swap_nwis;

        if new_rows > 1400000 and new_rows > old_rows - 100000 then
            pass_fail := 'PASS';
        else
            pass_fail := 'FAIL';
            $IF $$empty_db $THEN
                  pass_fail := 'PASS empty_db';
              $END
        end if;
          situation := pass_fail || ': table comparison for station: was ' || trim(to_char(old_rows, '999,999,999')) || ', now ' || trim(to_char(new_rows, '999,999,999'));
          dbms_output.put_line(situation);
          if pass_fail = 'FAIL' and message is null then
             message := situation;
          end if;

--      select count(*) into old_rows from series_catalog;
--      query := 'select count(*) from series_catalog' || suffix;
--      open c for query;
--      fetch c into new_rows;
--      close c;
--
--      if new_rows > 14000000 and new_rows > old_rows - 2000000 then
--         pass_fail := 'PASS';
--      else
--         pass_fail := 'FAIL';
--           $IF $$empty_db $THEN
--              pass_fail := 'PASS empty_db';
--           $END
--      end if;
--      situation := pass_fail || ': table comparison for series_catalog: was ' || trim(to_char(old_rows, '999,999,999')) || ', now ' || trim(to_char(new_rows, '999,999,999'));
--      dbms_output.put_line(situation);
--      if pass_fail = 'FAIL' and message is null then
--         message := situation;
--      end if;
--
--      select count(*) into old_rows from qwportal_summary;
--      query := 'select count(*) from qwportal_summary' || suffix;
--      open c for query;
--      fetch c into new_rows;
--      close c;
--
--      if new_rows > 20000 then
--         pass_fail := 'PASS';
--      else
--         pass_fail := 'FAIL';
--           $IF $$empty_db $THEN
--              pass_fail := 'PASS empty_db';
--           $END
--      end if;
--      situation := pass_fail || ': table comparison for qwportal_summary: was ' || trim(to_char(old_rows, '999,999,999')) || ', now ' || trim(to_char(new_rows, '999,999,999'));
--      dbms_output.put_line(situation);
--      if pass_fail = 'FAIL' and message is null then
--         message := situation;
--      end if;

    exception
        when others then
            message := 'FAIL validation: ' || SQLERRM;
            dbms_output.put_line(message);

    end validate;

    procedure install is
    begin

        dbms_output.put_line('installing...');

        dbms_output.put_line('station');
      	execute immediate 'alter table station exchange partition station_nwis with table station_swap_nwis including indexes';
        dbms_output.put_line('pc_result');
      	execute immediate 'alter table pc_result exchange partition pc_result_nwis with table pc_result_swap_nwis including indexes';
        dbms_output.put_line('station_sum');
		execute immediate 'alter table station_sum exchange partition station_sum_nwis with table station_sum_swap_nwis including indexes';
        dbms_output.put_line('pc_result_sum');
		execute immediate 'alter table pc_result_sum exchange partition pc_result_sum_nwis with table pc_result_sum_swap_nwis including indexes';
        dbms_output.put_line('pc_result_ct_sum');
		execute immediate 'alter table pc_result_ct_sum exchange partition pc_result_ct_sum_nwis with table pc_result_ct_sum_swap_nwis including indexes';
        dbms_output.put_line('pc_result_nr_sum');
		execute immediate 'alter table pc_result_nr_sum exchange partition pc_res_nr_sum_nwis with table pc_result_nr_sum_swap_nwis including indexes';
        dbms_output.put_line('characteristic_name');
		execute immediate 'alter table characteristic_name exchange partition char_name_nwis with table characteristic_name_swap_nwis including indexes';
        dbms_output.put_line('characteristic_type');
		execute immediate 'alter table characteristic_type exchange partition char_type_nwis with table characteristic_type_swap_nwis including indexes';
        dbms_output.put_line('country');
		execute immediate 'alter table country exchange partition country_nwis with table country_swap_nwis including indexes';
        dbms_output.put_line('county');
		execute immediate 'alter table county exchange partition county_nwis with table county_swap_nwis including indexes';
        dbms_output.put_line('organization');
		execute immediate 'alter table organization exchange partition organization_nwis with table organization_swap_nwis including indexes';
        dbms_output.put_line('sample_media');
		execute immediate 'alter table sample_media exchange partition sample_media_nwis with table sample_media_swap_nwis including indexes';
        dbms_output.put_line('site_type');
		execute immediate 'alter table site_type exchange partition site_type_nwis with table site_type_swap_nwis including indexes';
        dbms_output.put_line('state');
		execute immediate 'alter table state exchange partition state_nwis with table state_swap_nwis including indexes';

    exception
        when others then
            message := 'FAIL install: ' || SQLERRM;
            dbms_output.put_line(message);

    end install;

--   procedure drop_old_stuff
--   is
--      to_drop cursor_type;
--      drop_query varchar2(4000) := 'select table_name from user_tables where ' || table_list ||
--            ' and substr(table_name, -5) <= to_char(to_number(substr(:current_suffix, 2) - 2), ''fm00000'')' ||
--            ' and substr(table_name, -5) <> ''00000''' ||
--               ' order by case when table_name like ''FA_STATION%'' then 2 else 1 end, table_name';
--
--      to_nocache cursor_type;
--      nocache_query varchar2(4000) := 'select table_name from user_tables where ' || table_list ||
--            ' and substr(table_name, -5) <= to_char(to_number(substr(:current_suffix, 2) - 1), ''fm00000'')' ||
--               ' order by case when table_name like ''FA_STATION%'' then 2 else 1 end, table_name';
--
--      drop_name varchar2(30);
--      nocache_name varchar2(30);
--      stmt      varchar2(80);
--   begin
--
--      dbms_output.put_line('drop_old_stuff...');
--
--      open to_drop for drop_query using suffix;
--      loop
--         fetch to_drop into drop_name;
--         exit when to_drop%NOTFOUND;
--         stmt := 'drop table ' || drop_name;
--         dbms_output.put_line('CLEANUP old stuff: ' || stmt);
--         execute immediate stmt;
--      end loop;
--      close to_drop;
--
--      open to_nocache for nocache_query using suffix;
--      loop
--         fetch to_nocache into nocache_name;
--         exit when to_nocache%NOTFOUND;
--         stmt := 'alter table ' || nocache_name || ' nocache';
--         dbms_output.put_line('CLEANUP old stuff: ' || stmt);
--         execute immediate stmt;
--      end loop;
--      close to_nocache;
--
--   exception
--      when others then
--         message := 'tried to drop ' || drop_name || ' : ' || SQLERRM;
--         dbms_output.put_line(message);
--
--   end drop_old_stuff;

    procedure main(mesg in out varchar2, success_notify in varchar2, failure_notify in varchar2) is
        email_subject varchar2(  80);
        email_notify  varchar2( 400);
    begin
        message := null;
        dbms_output.enable(100000);

        dbms_output.put_line('started nad table transformation.');

		if message is null then create_station;                 end if;
        if message is null then create_pc_result;               end if;
        if message is null then create_summaries;               end if;
        if message is null then create_code_tables;             end if;
        if message is null then create_station_indexes;         end if;
        if message is null then create_result_indexes;          end if;
        if message is null then create_station_sum_indexes;     end if;
        if message is null then create_result_sum_indexes;      end if;
        if message is null then create_result_ct_sum_indexes;   end if;
        if message is null then create_result_nr_sum_indexes;	end if;
        if message is null then analyze_tables;                 end if;
		if message is null then validate;                     end if;
          
        if message is null then
            install;
        else
            dbms_output.put_line('completed. (failed)');
            dbms_output.put_line('errors occurred.');
            dbms_output.put_line('nad load FAILED');
            email_notify := failure_notify;
        end if;

        if message is null then
--            drop_old_stuff;
--            if message is null then
                dbms_output.put_line('completed. (success)');
			else
                dbms_output.put_line('completed. (failed)');
                dbms_output.put_line('errors occurred.');
--             end if;
        end if;
      
        mesg := message;

    end main;
end create_nad_objects;
/
