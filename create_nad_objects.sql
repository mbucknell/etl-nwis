show user;
select * from global_name;

create or replace package create_nad_objects
   authid definer
   as
   procedure main(mesg in out varchar2, success_notify in varchar2, failure_notify in varchar2);
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

   message varchar2(600);
   suffix varchar2(10);

   type cleanuptable is table of varchar2(80) index by binary_integer;
   cleanup cleanuptable;

   table_list varchar2(4000 char) := 'translate(table_name, ''0123456789'', ''0000000000'') in ' ||
                                     '(''FA_REGULAR_RESULT_00000'',''FA_STATION_00000'',''SERIES_CATALOG_00000'',''QWPORTAL_SUMMARY_00000'',' ||
                                      '''NWIS_STATION_SUM_00000'',''NWIS_RESULT_SUM_00000'',''NWIS_RESULT_CT_SUM_00000'',''NWIS_RESULT_NR_SUM_00000'',' ||
                                      '''NWIS_LCTN_LOC_00000'',''PUBLIC_SRSNAMES_00000'',''NWIS_DI_ORG_00000'',''CHARACTERISTICNAME_00000'',' ||
                                      '''CHARACTERISTICTYPE_00000'',''COUNTRY_00000'',''COUNTY_00000'',''ORGANIZATION_00000'',''SAMPLEMEDIA_00000'',' ||
                                      '''SITETYPE_00000'',''STATE_00000'')';
                                      
   type cursor_type is ref cursor;

   procedure determine_suffix
   is
      drop_remnants cursor_type;
      query         varchar2(4000) := 'select table_name from user_tables where ' || table_list ||
                                      ' and substr(table_name, -5) = substr(:current_suffix, 2) order by case when table_name like ''FA_STATION%'' then 2 else 1 end, table_name';

      drop_name varchar2(30);
      stmt      varchar2(80);

   begin

      select '_' || to_char(nvl(max(to_number(substr(table_name, length('FA_REGULAR_RESULT_') + 1)) + 1), 1), 'fm00000')
        into suffix from user_tables
        where translate(table_name, '0123456789', '0000000000') = 'FA_REGULAR_RESULT_00000';

      dbms_output.put_line('using ''' || suffix || ''' for suffix.');

      open drop_remnants for query using suffix;
      loop
         fetch drop_remnants into drop_name;
         exit when drop_remnants%NOTFOUND;
         stmt := 'drop table ' || drop_name || ' cascade constraints purge';
         dbms_output.put_line('CLEANUP remnants: ' || stmt);
         execute immediate stmt;
      end loop;

   exception
      when others then
         message := 'FAIL to determine suffix: ' || SQLERRM;
         dbms_output.put_line(message);
   end determine_suffix;

   procedure create_regular_result
   is
   begin

      dbms_output.put_line('creating regular_result...');

      execute immediate '
      create table fa_regular_result' || suffix || q'! parallel 4 compress pctfree 0 nologging
      partition by range(activity_start_date_time)
      (
         partition fa_regular_result_pre_1990 values less than (to_date('01-JAN-1990', 'DD-MON-YYYY')),
         partition fa_regular_result_1990     values less than (to_date('01-JAN-1991', 'DD-MON-YYYY')),
         partition fa_regular_result_1991     values less than (to_date('01-JAN-1992', 'DD-MON-YYYY')),
         partition fa_regular_result_1992     values less than (to_date('01-JAN-1993', 'DD-MON-YYYY')),
         partition fa_regular_result_1993     values less than (to_date('01-JAN-1994', 'DD-MON-YYYY')),
         partition fa_regular_result_1994     values less than (to_date('01-JAN-1995', 'DD-MON-YYYY')),
         partition fa_regular_result_1995     values less than (to_date('01-JAN-1996', 'DD-MON-YYYY')),
         partition fa_regular_result_1996     values less than (to_date('01-JAN-1997', 'DD-MON-YYYY')),
         partition fa_regular_result_1997     values less than (to_date('01-JAN-1998', 'DD-MON-YYYY')),
         partition fa_regular_result_1998     values less than (to_date('01-JAN-1999', 'DD-MON-YYYY')),
         partition fa_regular_result_1999     values less than (to_date('01-JAN-2000', 'DD-MON-YYYY')),
         partition fa_regular_result_2000     values less than (to_date('01-JAN-2001', 'DD-MON-YYYY')),
         partition fa_regular_result_2001     values less than (to_date('01-JAN-2002', 'DD-MON-YYYY')),
         partition fa_regular_result_2002     values less than (to_date('01-JAN-2003', 'DD-MON-YYYY')),
         partition fa_regular_result_2003     values less than (to_date('01-JAN-2004', 'DD-MON-YYYY')),
         partition fa_regular_result_2004     values less than (to_date('01-JAN-2005', 'DD-MON-YYYY')),
         partition fa_regular_result_2005     values less than (to_date('01-JAN-2006', 'DD-MON-YYYY')),
         partition fa_regular_result_2006     values less than (to_date('01-JAN-2007', 'DD-MON-YYYY')),
         partition fa_regular_result_2007     values less than (to_date('01-JAN-2008', 'DD-MON-YYYY')),
         partition fa_regular_result_2008     values less than (to_date('01-JAN-2009', 'DD-MON-YYYY')),
         partition fa_regular_result_2009     values less than (to_date('01-JAN-2010', 'DD-MON-YYYY')),
         partition fa_regular_result_2010     values less than (to_date('01-JAN-2011', 'DD-MON-YYYY')),
         partition fa_regular_result_2011     values less than (to_date('01-JAN-2012', 'DD-MON-YYYY')),
         partition fa_regular_result_last     values less than (maxvalue)
      )
      as
      select /*+ full(y) parallel(y, 4) full(aqfr) parallel(aqfr, 4) use_hash(y) use_hash(aqfr) */
         y.FK_STATION,
         y.ACTIVITY_START_DATE_TIME,
         y.ACT_START_TIME_ZONE,
         y.CHARACTERISTIC_NAME,
         y.SRSID,
         y.RESULT_VALUE,
         y.RESULT_UNIT,
         y.RESULT_VALUE_TEXT,
         y.SAMPLE_FRACTION_TYPE,
         y.RESULT_VALUE_TYPE,
         y.STATISTIC_TYPE,
         y.RESULT_VALUE_STATUS,
         y.WEIGHT_BASIS_TYPE,
         y.TEMPERATURE_BASIS_LEVEL,
         y.DURATION_BASIS,
         y.ANALYTICAL_PROCEDURE_SOURCE,
         y.ANALYTICAL_PROCEDURE_ID,
         y.LAB_NAME,
         y.ANALYSIS_DATE_TIME,
         y.ANALYSIS_TIME_ZONE,
         y.LOWER_QUANTITATION_LIMIT,
         y.UPPER_QUANTITATION_LIMIT,
         y.DETECTION_LIMIT,
         y.DETECTION_LIMIT_DESCRIPTION,
         y.LAB_REMARK,
         y.PARTICLE_SIZE,
         y.PRECISION,
         y.CONFIDENCE_LEVEL,
         y.DILUTION_INDICATOR,
         y.RECOVERY_INDICATOR,
         y.CORRECTION_INDICATOR,
         y.ACTIVITY_ID,
         y.ACTIVITY_TYPE,
         y.ACTIVITY_INTENT,
         y.ACTIVITY_STOP_DATE_TIME,
         y.ACT_STOP_TIME_ZONE,
         y.ACTIVITY_DEPTH,
         y.ACTIVITY_DEPTH_UNIT,
         y.ACTIVITY_UPPER_DEPTH,
         y.ACTIVITY_LOWER_DEPTH,
         y.UPR_LWR_DEPTH_UNIT,
         y.RESULT_COMMENT,
         y.CAS_NUMBER,
         y.ITIS_NUMBER,
         y.ACTIVITY_COMMENT,
         y.ACTIVITY_DEPTH_REF_POINT,
         y.RESULT_DETECTION_CONDITION_TX,
         y.SAMPLE_TISSUE_TAXONOMIC_NAME,
         y.ACTIVITY_MEDIA_NAME,
         y.ACTIVITY_MEDIA_SUBDIV_NAME,
         y.ANALYSIS_PREP_DATE_TX,
         aqfr.SAMPLE_AQFR_NAME,
         y.HYDROLOGIC_CONDITION_NAME,
         y.HYDROLOGIC_EVENT_NAME,
         y.PROJECT_ID ,
         y.ACTIVITY_UPRLWR_DEPTH_REF_PT,
         y.SAMPLE_TISSUE_ANATOMY_NAME,
         y.PARAMETER_CODE,
         y.CHARACTERISTIC_TYPE,
         y.ACTIVITY_CONDUCTING_ORG,
         y.ANALYTICAL_METHOD_NAME,
         y.ANALYTICAL_METHOD_CITATION,
         y.ACTIVITY_START_DATE_TX,
         y.ACTIVITY_START_TIME_TX,
         y.ACT_START_TIME_ZONE_LOCAL,
         y.ACTIVITY_START_DATE_TX_UTC,
         y.ACTIVITY_START_TIME_TX_UTC,
         y.ACT_START_TIME_ZONE_UTC,
         y.ACTIVITY_STOP_DATE_TX,
         y.ACTIVITY_STOP_TIME_TX,
         y.ACT_STOP_TIME_ZONE_LOCAL,
         y.ACTIVITY_STOP_DATE_TX_UTC,
         y.ACTIVITY_STOP_TIME_TX_UTC,
         y.ACT_STOP_TIME_ZONE_UTC,
         y.SAMPLE_COLLECT_EQUIP_NAME,
         y.SAMPLE_COLLECT_METHOD_NAME,
         y.SAMPLE_COLLECT_METHOD_ID,
         y.SAMPLE_COLLECT_METHOD_CTX,
         y.SAMPLE_ID,
         cast(case
                when nemi.method_id is not null
                  then
                    case nemi.method_type
                      when 'analytical'
                        then 'https://www.nemi.gov/methods/method_summary/' || method_id || '/'
                      when 'statistical'
                        then 'https://www.nemi.gov/methods/sams_method_summary/' || method_id || '/'
                    end
                else 
                  null 
              end as varchar2(256 char)) nemi_url
      from
        (select /*+ full(r) full(samp) full(site) full(parameter)
                    full(z_param_alias) full(tu) full(wqx_medium_cd) full(body_part) full(parm) full(fxd)
                    full(proto_org) full(proto_org2) full(meth) full(z_parm_meth) full(nwis_wqx_rpt_lev_cd)
                    full(val_qual_cd1) full(val_qual_cd2) full(val_qual_cd3) full(val_qual_cd4) full(val_qual_cd5) full(dist)
                    use_hash(r) use_hash(samp) use_hash(site) use_hash(parameter)
                    use_hash(z_param_alias) use_hash(tu) use_hash(wqx_medium_cd) use_hash(body_part) use_hash(parm) use_hash(fxd)
                    use_hash(proto_org) use_hash(proto_org2) use_hash(meth) use_hash(z_parm_meth) use_hash(nwis_wqx_rpt_lev_cd)
                    use_hash(val_qual_cd1) use_hash(val_qual_cd2) use_hash(val_qual_cd3) use_hash(val_qual_cd4) use_hash(val_qual_cd5) use_hash(dist) */
            site.site_id as FK_STATION,
            samp.sample_start_dt as ACTIVITY_START_DATE_TIME,
            case when samp.SAMPLE_START_DT is not null and samp.SAMPLE_START_SG in ('h','m') then samp.SAMPLE_START_TIME_DATUM_CD
                 else null
            end as ACT_START_TIME_ZONE,
            cast(parm.srsname as varchar2(500)) as CHARACTERISTIC_NAME,
            cast(parm.srsid as number(10)) as SRSID,
            cast(null as number) as RESULT_VALUE,
            parm.parm_unt_tx as RESULT_UNIT,
            nvl(fxd.fxd_tx,
                   case when r.REMARK_CD in ('U', 'M', 'N', '<', '>') then null
                        when r.REMARK_CD in ('R', 'V', 'S', 'E', 'A') or r.REMARK_CD is null then r.result_va
                        else r.result_va
                   end) as RESULT_VALUE_TEXT,
            parm.parm_frac_tx as SAMPLE_FRACTION_TYPE,
            case when r.RESULT_MD is null then 'Calculated'
                 when r.REMARK_CD='E' then'Estimated'
                 else 'Actual'
            end as RESULT_VALUE_TYPE,
            nvl(parm.parm_stat_tx,
                    case when r.REMARK_CD = 'S' then 'MPN'
                         when r.REMARK_CD = 'A' then 'mean'
                         else null
                    end) as STATISTIC_TYPE,
            case when r.DQI_CD = 'S' then 'Preliminary'
                 when r.DQI_CD = 'A' then 'Historical'
                 when r.DQI_CD = 'R' then 'Accepted'
                 else null end as RESULT_VALUE_STATUS,
            parm.parm_wt_tx as WEIGHT_BASIS_TYPE,
            parm.parm_temp_tx  as TEMPERATURE_BASIS_LEVEL,
            parm.parm_tm_tx as DURATION_BASIS,
            case when r.METH_CD is not null then cast('USGS' as varchar2(4))
                 else null
            end as ANALYTICAL_PROCEDURE_SOURCE,
            r.meth_cd as ANALYTICAL_PROCEDURE_ID,
            proto_org.proto_org_nm as LAB_NAME,
            case when r.ANL_DT is not null then
                    substr(ANL_DT, 1, 4) || '-' || substr(ANL_DT, 5, 2) || '-' || substr(ANL_DT, 7, 2)
                 else null
            end as ANALYSIS_DATE_TIME,
            cast(null as varchar(2)) as ANALYSIS_TIME_ZONE,
            cast(null as varchar(2)) as LOWER_QUANTITATION_LIMIT,
            cast(null as varchar(2)) as UPPER_QUANTITATION_LIMIT,
            case when r.remark_cd = '<' and r.rpt_lev_va is null then r.result_va
                 when r.remark_cd = '<' and to_number(r.result_unrnd_va) > to_number(r.rpt_lev_va) then r.result_va
                 when r.remark_cd = '>' then r.result_va
                 when r.remark_cd in ('N', 'U') and r.rpt_lev_va is not null then r.rpt_lev_va
                 when r.remark_cd = 'M' and r.rpt_lev_va is null and r.result_unrnd_va is null then null
                 when r.remark_cd = 'M' and r.rpt_lev_va is null and r.result_unrnd_va is not null then nvl(z_parm_meth.multiplier, parm.multiplier)
                 when nwis_wqx_rpt_lev_cd.rpt_lev_cd is not null then r.rpt_lev_va
                 else null
            end as DETECTION_LIMIT,
            case when r.remark_cd = '<' and r.rpt_lev_va is null then 'Historical Lower Reporting Limit'
                 when r.remark_cd = '<' and to_number(r.result_unrnd_va) > to_number(r.rpt_lev_va) then 'Elevated Detection Limit'
                 when r.remark_cd = '<' and nwis_wqx_rpt_lev_cd.rpt_lev_cd is not null then nwis_wqx_rpt_lev_cd.wqx_rpt_lev_nm
                 when r.remark_cd = '>' then 'Upper Reporting Limit'
                 when r.remark_cd = 'M' and r.rpt_lev_va is null and r.result_unrnd_va is null then null
                 when r.remark_cd = 'M' and r.rpt_lev_va is null and r.result_unrnd_va is not null then 'Lower Quantitation Limit'
                 when r.remark_cd = 'M' and r.rpt_lev_va is not null then nwis_wqx_rpt_lev_cd.wqx_rpt_lev_nm
                 when r.rpt_lev_va is not null then nwis_wqx_rpt_lev_cd.wqx_rpt_lev_nm
                 else null
            end as DETECTION_LIMIT_DESCRIPTION,
            trim( val_qual_cd1.val_qual_nm ||
                  val_qual_cd2.val_qual_nm ||
                  val_qual_cd3.val_qual_nm ||
                  val_qual_cd4.val_qual_nm ||
                  val_qual_cd5.val_qual_nm ||
                  case when r.remark_cd = 'R' then 'Result below sample specific critical level.'
                       else null
                  end) as LAB_REMARK,
            trim(parm.parm_size_tx) as PARTICLE_SIZE,
            r.lab_std_va as PRECISION,
            cast(null as varchar(2)) as CONFIDENCE_LEVEL,
            cast(null as varchar(2)) as DILUTION_INDICATOR,
            cast(null as varchar(2)) as RECOVERY_INDICATOR,
            cast(null as varchar(2)) as CORRECTION_INDICATOR,
            samp.nwis_host||'.'||samp.qw_db_no||'.'||samp.record_no as ACTIVITY_ID,
            case when samp.samp_type_cd = 'A' then 'Not determined'
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
            end as ACTIVITY_TYPE,
            cast(null as varchar(80)) as ACTIVITY_INTENT,
            to_date(samp.sample_end_dt, 'YYYY-MM-DD HH24:MI:SS') as ACTIVITY_STOP_DATE_TIME,
            case when samp.sample_end_dt is not null and samp.sample_end_sg in ('h', 'm') then samp.sample_start_time_datum_cd
                 else null
            end as ACT_STOP_TIME_ZONE,
            coalesce(parameter.V00003, parameter.V00098, parameter.V78890, parameter.V78891) as ACTIVITY_DEPTH,
            case when parameter.V00003 is not null then 'feet'
                 when parameter.V00098 is not null then 'meters'
                 when parameter.V78890 is not null then 'feet'
                 when parameter.V78891 is not null then 'meters'
                 else null
            end as ACTIVITY_DEPTH_UNIT,
            coalesce(parameter.V72015, parameter.V82047) as ACTIVITY_UPPER_DEPTH,
            case when parameter.V72015 is not null then parameter.V72016
                 when parameter.V82047 is not null then parameter.V82048
                 when parameter.V72016 is not null then parameter.V72016
                 when parameter.V82048 is not null then parameter.V82048
                 else null
            end as ACTIVITY_LOWER_DEPTH,
            case when parameter.V72015 is not null then 'feet'
                 when parameter.V82047 is not null then 'meters'
                 when parameter.V72016 is not null then 'feet'
                 when parameter.V82048 is not null then 'meters'
                 else null
            end as UPR_LWR_DEPTH_UNIT,
            trim(r.result_lab_cm_tx) RESULT_COMMENT,
            cast(parm.casrn as varchar2(12)) as CAS_NUMBER,
            tu.tu_id as ITIS_NUMBER,
            trim(samp.sample_lab_cm_tx) as ACTIVITY_COMMENT,
            case when parameter.V00003 is not null or parameter.V00098 is not null then null
                 when parameter.V78890 is not null or parameter.V78891 is not null then cast('Below mean sea level' as varchar2(20))
                 else null
            end as ACTIVITY_DEPTH_REF_POINT,
            case when r.remark_cd = 'U' then 'Not Detected'
                 when r.remark_cd = 'V' then 'Systematic Contamination'
                 when r.remark_cd = 'S' then null
                 when r.remark_cd = 'M' then 'Detected Not Quantified'
                 when r.remark_cd = 'N' then 'Detected Not Quantified'
                 when r.remark_cd = 'A' then null
                 when r.remark_cd = '<' then 'Not Detected'
                 when r.remark_cd = '>' then 'Present Above Quantification Limit'
                 else null
            end as RESULT_DETECTION_CONDITION_TX,
            case when parm.PARM_MEDIUM_TX = 'Biological Tissue' then tu.composite_tu_name
            end as SAMPLE_TISSUE_TAXONOMIC_NAME,
            wqx_medium_cd.WQX_ACT_MED_NM as ACTIVITY_MEDIA_NAME,
            wqx_medium_cd.WQX_ACT_MED_SUB as ACTIVITY_MEDIA_SUBDIV_NAME,
            case when r.prep_dt is not null then substr(r.prep_dt, 1, 4) || '-' || substr(r.prep_dt, 5, 2) || '-' || substr(r.prep_dt, 7, 2)
                 else null
            end as ANALYSIS_PREP_DATE_TX,
            samp.aqfr_cd,  /* these next two needed for join but are not in final result */
            site.state_cd,
            hyd_cond_cd.hyd_cond_nm as HYDROLOGIC_CONDITION_NAME,
            hyd_event_cd.hyd_event_nm as HYDROLOGIC_EVENT_NAME,
            nvl(parameter.v71999_fxd_nm, samp.project_cd) as PROJECT_ID,
              --This is the way Informatica does it.  Above is theoretically equal and I think easier to read
              --case when parameter.V71999 = 15 then 'NAWQA'
              --     when parameter.V71999 = 20 then 'NASQAN'
              --     when parameter.V71999 = 25 then 'NMN'
              --     when parameter.V71999 = 30 then substr(P71999_fxd_tx parameter.v1999_fxd_tx, 1, 35)
              --     when parameter.V71999 = 35 then 'RASA'
              --     else substr(samp.project_cd, 1, 35) as PROJECT_ID,
            case when parameter.V72015 is not null then 'Below land-surface datum'
                 when parameter.V82047 is not null then ''
                 when parameter.V72016 is not null then 'Below land-surface datum'
                 when parameter.V82048 is not null then ''
                 else null
            end as ACTIVITY_UPRLWR_DEPTH_REF_PT,
            case when parm.parm_medium_tx = 'Biological Tissue' then body_part.body_part_nm
                 else null
            end as SAMPLE_TISSUE_ANATOMY_NAME,
            r.parameter_cd as PARAMETER_CODE,
            parm.parm_seq_grp_nm as CHARACTERISTIC_TYPE,
            coalesce(proto_org2.proto_org_nm, samp.coll_ent_cd) as ACTIVITY_CONDUCTING_ORG,
            meth.meth_nm as ANALYTICAL_METHOD_NAME,
            meth.cit_nm ANALYTICAL_METHOD_CITATION,
            case when samp.sample_start_sg in ('m', 'h', 'D') then to_char(samp.sample_start_dt, 'YYYY-MM-DD')
                 when samp.sample_start_sg = 'M' then to_char(samp.sample_start_dt, 'YYYY-MM')
                 when samp.sample_start_sg = 'Y' then to_char(samp.sample_start_dt, 'YYYY')
                 else null
            end as ACTIVITY_START_DATE_TX,
            case when samp.sample_start_sg in ('m', 'h') then to_char(samp.sample_start_dt, 'HH24:MI:SS')
                 else null
            end as ACTIVITY_START_TIME_TX,
            case when samp.SAMPLE_START_DT is not null and samp.SAMPLE_START_SG in ('h','m') then lu_tz.tz_utc_offset_tm
                 else null
            end as ACT_START_TIME_ZONE_LOCAL,/*here dave*/
            case when samp.sample_start_sg in ('m', 'h', 'D') then to_char(samp.sample_utc_start_dt, 'YYYY-MM-DD')
                 when samp.sample_start_sg = 'M' then to_char(samp.sample_utc_start_dt, 'YYYY-MM')
                 when samp.sample_start_sg = 'Y' then to_char(samp.sample_utc_start_dt, 'YYYY')
                 else null
            end as ACTIVITY_START_DATE_TX_UTC,
            case when samp.sample_start_sg in ('m', 'h') then to_char(samp.sample_utc_start_dt, 'HH24:MI:SS')
                 else null
            end as ACTIVITY_START_TIME_TX_UTC,
            case when samp.SAMPLE_UTC_START_DT is not null and samp.SAMPLE_START_SG in ('h','m') then '+00:00'
                 else null
            end as ACT_START_TIME_ZONE_UTC,
            case when samp.sample_end_sg in ('m', 'h', 'D') then substr(samp.sample_end_dt, 1, 10)
                 when samp.sample_end_sg = 'M' then substr(samp.sample_end_dt, 1, 7)
                 when samp.sample_end_sg = 'Y' then substr(samp.sample_end_dt, 1, 4)
                 else null
            end as ACTIVITY_STOP_DATE_TX,
            case when samp.sample_end_sg in ('m', 'h') then substr(samp.sample_end_dt,12) else null end as ACTIVITY_STOP_TIME_TX,
           case when samp.sample_end_dt is not null and samp.sample_end_sg in ('h', 'm') then lu_tz.tz_utc_offset_tm
                 else null
            end ACT_STOP_TIME_ZONE_LOCAL,/*here dave*/
            case when samp.sample_end_sg in ('m', 'h', 'D') then substr(samp.sample_utc_end_dt, 1, 10)
                 when samp.sample_end_sg = 'M' then substr(samp.sample_utc_end_dt, 1, 7)
                 when samp.sample_end_sg = 'Y' then substr(samp.sample_utc_end_dt, 1, 4)
                 else null
            end as ACTIVITY_STOP_DATE_TX_UTC,
            case when samp.sample_end_sg in ('m', 'h') then substr(samp.sample_utc_end_dt,12) else null end as ACTIVITY_STOP_TIME_TX_UTC,
            case when samp.sample_utc_end_dt is not null and samp.sample_end_sg in ('h', 'm') then '+00:00'
                 else null
            end ACT_STOP_TIME_ZONE_UTC,
            case when parameter.v84164_fxd_tx is not null and parameter.v82398_fxd_tx is not null
                 then parameter.v84164_fxd_tx
                 else null
            end as SAMPLE_COLLECT_EQUIP_NAME,
            case when parameter.v84164_fxd_tx is not null and parameter.v82398_fxd_tx is not null
                 then parameter.v82398_fxd_tx
                 else null
            end as SAMPLE_COLLECT_METHOD_NAME,
            case when parameter.v84164_fxd_tx is not null and parameter.v82398_fxd_tx is not null
                 then cast(parameter.V82398 as number(7))
                 else null
            end as SAMPLE_COLLECT_METHOD_ID,
            case when parameter.v84164_fxd_tx is not null and parameter.v82398_fxd_tx is not null
                 then cast('USGS parameter code 82398' as varchar2(25))
                 else null
            end as SAMPLE_COLLECT_METHOD_CTX,
            samp.sample_id as SAMPLE_ID
         from
            nwis_ws_star.qw_result  r,
            nwis_ws_star.qw_sample  samp,
            (select tz_cd, tz_utc_offset_tm
               from lu_tz
              where tz_cd is not null
             union 
             select tz_dst_cd, tz_dst_utc_offset_tm tz_utc_offset_tm
               from lu_tz
              where tz_dst_cd is not null) lu_tz,
            nwis_ws_star.sitefile   site,
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
            from
              (select /*+ full(p1) parallel(p1, 4) */
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
               from
                  nwis_ws_star.qw_result p1
               where
                  result_web_cd = 'Y' and
                  parameter_cd in ('71999', '50280', '72015', '82047', '72016', '82048', '00003', '00098', '78890', '78891', '82398', '84164')
               group by
                  sample_id) p2,
              (select fxd_nm v71999_fxd_nm, fxd_va from fxd where parm_cd = '71999') fxd_71999,
              (select fxd_tx v82398_fxd_tx, fxd_va from fxd where parm_cd = '82398') fxd_82398,
              (select fxd_tx v84164_fxd_tx, fxd_va from fxd where parm_cd = '84164') fxd_84164
            where
               p2.v71999 = fxd_71999.fxd_va(+) and
               p2.v82398 = fxd_82398.fxd_va(+) and
               p2.v84164 = fxd_84164.fxd_va(+)) parameter,
           (select
               tu_id,
               trim(tu_1_nm) ||
               case when trim(tu_2_cd) is not null then ' ' || trim(tu_2_cd) end ||
               case when trim(tu_2_nm) is not null then ' ' || trim(tu_2_nm) end ||
               case when trim(tu_3_cd) is not null then ' ' || trim(tu_3_cd) end ||
               case when trim(tu_3_nm) is not null then ' ' || trim(tu_3_nm) end ||
               case when trim(tu_4_cd) is not null then ' ' || trim(tu_4_cd) end ||
               case when trim(tu_4_nm) is not null then ' ' || trim(tu_4_nm) end AS composite_tu_name
            from
               tu) tu,
           (select
               trim(WQX_ACT_MED_NM)  AS wqx_act_med_nm ,
               trim(wqx_act_med_sub) AS wqx_act_med_sub,
               trim(nwis_medium_cd) medium_cd
            from
               nwis_wqx_medium_cd) wqx_medium_cd,
           (select
               trim(body_part_nm) body_part_nm,
               trim(body_part_id) body_part_id
            from
               body_part) body_part,
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
            from
               lu_parm a,
               lu_parm_seq_grp_cd b,
              (select
                  parm_cd,
                  max(case when parm_alias_cd = 'SRSNAME' then parm_alias_nm else null end) AS srsname,
                  max(case when parm_alias_cd = 'SRSID'   then parm_alias_nm else null end) AS srsid  ,
                  max(case when parm_alias_cd = 'CASRN'   then parm_alias_nm else null end) AS casrn
               from
                  lu_parm_alias
               group by
                  parm_cd
               having
                  max(case when parm_alias_cd = 'SRSNAME' then parm_alias_nm else null end) is not null) z_parm_alias,
              (select
                  decode(REGEXP_INSTR(PARM_METH_RND_TX, '[1-9]', 1, 1),
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
               from
                  lu_parm_meth
               where
                  meth_cd is null) z_parm_meth2
            where
               a.parm_public_fg = 'Y' and
               a.parm_seq_grp_cd = b.parm_seq_grp_cd(+) and
               a.parm_cd = z_parm_alias.parm_cd and
               a.parm_cd = z_parm_meth2.parm_cd(+)) parm,
           (select
               fxd_tx,
               parm_cd,
               fxd_va
            from
               fxd) fxd,
           (select
               proto_org_nm,
               proto_org_cd
            from
               proto_org) proto_org,
           (select
               proto_org_nm,
               proto_org_cd
            from
               proto_org) proto_org2,
           (select /*+ full(meth1) full(z_cit_meth) use_hash(meth1) use_hash(z_cit_meth) */
               meth1.meth_cd,
               meth1.meth_nm,
               z_cit_meth.cit_nm
            from
               meth meth1,
              (select meth_cd, min(cit_nm) cit_nm from z_cit_meth group by meth_cd) z_cit_meth
            where
               meth1.meth_cd = z_cit_meth.meth_cd(+)) meth,
           (select
               decode(REGEXP_INSTR(PARM_METH_RND_TX, '[1-9]', 1, 1),
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
            from
               lu_parm_meth) z_parm_meth,
           (select rpt_lev_cd, wqx_rpt_lev_nm from nwis_wqx_rpt_lev_cd) nwis_wqx_rpt_lev_cd,
           (select val_qual_nm || '. ' val_qual_nm, val_qual_cd from val_qual_cd) val_qual_cd1,
           (select val_qual_nm || '. ' val_qual_nm, val_qual_cd from val_qual_cd) val_qual_cd2,
           (select val_qual_nm || '. ' val_qual_nm, val_qual_cd from val_qual_cd) val_qual_cd3,
           (select val_qual_nm || '. ' val_qual_nm, val_qual_cd from val_qual_cd) val_qual_cd4,
           (select val_qual_nm || '. ' val_qual_nm, val_qual_cd from val_qual_cd) val_qual_cd5,
           (select trim(hyd_event_cd) hyd_event_cd, trim(hyd_event_nm) hyd_event_nm from hyd_event_cd) hyd_event_cd,
           (select trim(hyd_cond_cd) hyd_cond_cd, trim(hyd_cond_nm) hyd_cond_nm from hyd_cond_cd) hyd_cond_cd,
           (select district_cd, host_name from nwis_district_cds_by_host) dist
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
            samp.SAMPLE_START_TIME_DATUM_CD = lu_tz.tz_cd(+)
         ) y,
         (select aqfr_cd, state_cd, trim(aqfr_nm) as SAMPLE_AQFR_NAME from aqfr) aqfr,
         wqp_nemi_nwis_crosswalk nemi
      where
         y.aqfr_cd  = aqfr.aqfr_cd (+) and
         y.state_cd = aqfr.state_cd(+) and
         trim(y.analytical_procedure_source) = nemi.analytical_procedure_source(+) and
         trim(y.analytical_procedure_id) = nemi.analytical_procedure_id(+)!' ;

        cleanup(1) := 'drop table FA_REGULAR_RESULT' || suffix || ' cascade constraints purge';
     exception
      when others then
         message := 'FAIL to create FA_REGULAR_RESULT: ' || SQLERRM;
         dbms_output.put_line(message);
   end create_regular_result;

   procedure create_station
   is
   begin

      dbms_output.put_line('creating station...');

      execute immediate
     'create table fa_station' || suffix || ' compress pctfree 0 nologging as
      select
         site_id as PK_ISN,
         agency_cd || ''-'' || site_no as STATION_ID,
         trim(station_nm) as STATION_NAME,
         ndcbh.organization_id,
         round(dec_lat_va , 7) as LATITUDE,
         round(dec_long_va, 7) as LONGITUDE,
         trim(map_scale_fc) as MAP_SCALE,
         case when alt_datum_cd is not null then case when alt_va = ''.'' then ''0'' else trim(alt_va) end else null end as ELEVATION,
         case when length(huc_cd) = 8 then huc_cd else null end as HYDROLOGIC_UNIT_CODE,
         trim(site_rmks_tx) as DESCRIPTION_TEXT,
         cast(''NWISWeb'' as varchar2(7)) as SOURCE_SYSTEM,
         dec_coord_datum_cd as HORIZ_DATUM_NAME,
         case when alt_va is not null then alt_datum_cd else null end as VERTICAL_DATUM_NAME,
         country.country_cd,
         country.country_name,
         state.state_cd,
         state.state_name,
         county.county_cd,
         county.county_name,
         postal.state_postal_cd,
         trim(land_net_ds) LAND_NET_DS,
         site_tp.station_type_name,
         vert.vertical_method_name,
         geo_meth.geopositioning_method,
         geo_accuracy.geopositioning_accuracy_value,
         geo_accuracy.geopositioning_accuracy_units,
         case when ALT_VA is not null and ALT_DATUM_CD is not null then trim(ALT_ACY_VA) else null end as VERTICAL_ACCURACY_VALUE,
         case when ALT_VA is not null and ALT_DATUM_CD is not null and ALT_ACY_VA is not null then
                    cast(''feet'' as varchar2(4))
               else null
         end as VERTICAL_ACCURACY_UNITS,
         nat_aqfr.nat_aqfr_name,
         aqfr_type.aqfr_type_name,
         to_number(drain_area_va) as DRAIN_AREA_MI2_VA,
         case when contrib_drain_area_va = ''.'' then 0 else to_number(to_char(contrib_drain_area_va)) end as CONTRIB_DRAIN_MI2_AREA_VA,
         aqfr.aqfr_name,
         to_number(case when well_depth_va in (''.'', ''-'') then ''0'' else well_depth_va end) as WELL_DEPTH_FT_BLW_LAND_SFC_VA,
         to_number(case when hole_depth_va in (''.'', ''-'') then ''0'' else hole_depth_va end) as HOLE_DEPTH_FT_BLW_LAND_SFC_VA,
         construction_dt as CONSTRUCTION_DATE_TX,
         trim(basin_cd) as BASIN_CD,
         case when ALT_VA is not null and ALT_DATUM_CD is not null then
                   cast(''feet'' as varchar2(4))
              else null
         end as ELEV_UNITS,
         ndcbh.organization_name,
          /* case when substr(STATION_TYPE_CD,10,1) = ''Y'' then ''Wastewater disposal''
               when substr(STATION_TYPE_CD, 8,1) = ''Y'' then ''Outfall''
               when substr(STATION_TYPE_CD,13,1) = ''Y'' then ''Water-use establishment''
               when substr(STATION_TYPE_CD, 9,1) = ''Y'' then ''Diversion''
               when substr(STATION_TYPE_CD, 1,1) = ''Y'' then ''River/Stream''
               when substr(STATION_TYPE_CD, 2,1) = ''Y'' then ''Lake''
               when substr(STATION_TYPE_CD, 5,1) = ''Y'' then ''Spring''
               when substr(STATION_TYPE_CD, 6,1) = ''Y'' then
                    case when gw_type_cd = ''W'' then ''Well''
                         when gw_type_cd = ''C'' then ''Gallery''
                         when gw_type_cd = ''I'' then ''Interconnected wells''
                         when gw_type_cd = ''M'' then ''Multiple wells''
                         when gw_type_cd = ''X'' then ''Test hole''
                         when gw_type_cd = ''T'' then ''Mine/mine discharge''
                         when gw_type_cd = ''D'' then ''Groundwater drain''
                         when gw_type_cd = ''P'' then ''Lake''
                         when gw_type_cd = ''H'' then ''Land''
                         when gw_type_cd = ''S'' then ''Spring''
                         when gw_type_cd = ''O'' then ''Land''
                         when gw_type_cd = ''E'' then ''Land''
                         else                       ''Well''
                     end
               when substr(STATION_TYPE_CD,11,1) = ''Y'' then ''Other-Ground Water''
               when substr(STATION_TYPE_CD,12,1) = ''Y'' then ''Other-Surface Water''
               when substr(STATION_TYPE_CD, 3,1) = ''Y'' then ''Estuary''
               when substr(STATION_TYPE_CD,14,1) = ''Y'' then ''Coastal''
               when substr(STATION_TYPE_CD, 7,1) = ''Y'' then ''National Air Monitoring Station''
               when substr(STATION_TYPE_CD, 4,1) = ''Y'' then ''Facility''
               else null
          end */ cast(NULL as varchar2(32)) as WQX_STATION_TYPE,
          mdsys.sdo_geometry(2001,8265,mdsys.sdo_point_type(round(dec_long_va, 7),round(dec_lat_va, 7), null), null, null) as GEOM,
          fips.state_fips,
          cast(site_tp.primary_site_type as varchar2(30)) primary_site_type
      from
          nwis_ws_star.sitefile sitefile,
         (select cast(''USGS-'' || state_postal_cd as varchar2(7)) as organization_id,
           ''USGS '' || STATE_NAME || '' Water Science Center'' as organization_name, host_name, district_cd
          from nwis_district_cds_by_host) ndcbh,
         (select cast(country_cd as varchar2(2)) as country_cd, country_nm as country_name from stage_country) country,
         (select cast(state_cd as varchar2(2)) as state_cd, state_nm as state_name, country_cd from stage_state) state,
         (select cast(county_cd as varchar2(3)) as county_cd, state_cd, country_cd, county_nm as county_name from stage_county) county,
         (select cast(state_post_cd as varchar2(2)) as state_postal_cd, state_cd, country_cd from stage_state) postal,
         (select
             a.site_tp_cd,
             case when a.site_tp_prim_fg = ''Y'' then a.site_tp_ln
                  else b.site_tp_ln || '': '' || a.site_tp_ln
             end as station_type_name,
             case when a.site_tp_prim_fg = ''Y'' then a.site_tp_ln
                  else b.site_tp_ln
             end as primary_site_type
          from
             site_tp a,
             site_tp b
          where
             substr(a.site_tp_cd, 1, 2) = b.site_tp_cd and
             b.site_tp_prim_fg = ''Y'') site_tp,
         (select nwis_name as vertical_method_name , nwis_code from nwis_misc_lookups where category = ''Altitude Method'') vert,
         (select nwis_name as geopositioning_method, nwis_code from nwis_misc_lookups where category = ''Lat/Long Method'') geo_meth,
         (select inferred_value as geopositioning_accuracy_value,
                 inferred_units as geopositioning_accuracy_units,
                 nwis_code from nwis_misc_lookups where category = ''Lat-Long Coordinate Accuracy'') geo_accuracy,
         (select nat_aqfr_nm as nat_aqfr_name, nat_aqfr_cd from nat_aqfr group by nat_aqfr_nm, nat_aqfr_cd) nat_aqfr,
         (select nwis_name as aqfr_type_name, nwis_code from nwis_misc_lookups where CATEGORY=''Aquifer Type Code'') aqfr_type,
         (select aqfr_nm as aqfr_name, state_cd, aqfr_cd from aqfr) aqfr,
         (select state_fips, state_cd from state_fips) fips
     where
       sitefile.DEC_LAT_VA   <> 0   and
       sitefile.DEC_LONG_VA  <> 0   and
       sitefile.site_web_cd  = ''Y''  and
       sitefile.db_no        = ''01'' and
       sitefile.site_tp_cd not in (''FA-WTP'', ''FA-WWTP'', ''FA-TEP'', ''FA-HP'')   and
       sitefile.nwis_host  not in (''fltlhsr001'', ''fltpasr001'', ''flalssr003'') and
       sitefile.nwis_host    =  ndcbh.host_name  and  /* host name must exist - no outer join */
       sitefile.district_cd  = ndcbh.district_cd and
       sitefile.country_cd   = country.country_cd(+) and
       sitefile.country_cd   = state.country_cd  (+) and
       sitefile.state_cd     = state.state_cd    (+) and
       sitefile.country_cd   = county.country_cd (+) and
       sitefile.state_cd     = county.state_cd   (+) and
       sitefile.county_cd    = county.county_cd  (+) and
       sitefile.country_cd   = postal.country_cd (+) and
       sitefile.state_cd     = postal.state_cd   (+) and
       sitefile.site_tp_cd   = site_tp.site_tp_cd(+) and
       sitefile.alt_meth_cd  = vert.nwis_code    (+) and
       sitefile.coord_meth_cd= geo_meth.nwis_code(+) and
       sitefile.coord_acy_cd = geo_accuracy.nwis_code(+) and
       sitefile.nat_aqfr_cd  = nat_aqfr.nat_aqfr_cd(+) and
       sitefile.aqfr_type_cd = aqfr_type.nwis_code(+) and
       sitefile.aqfr_cd      = aqfr.aqfr_cd(+) and
       sitefile.state_cd     = aqfr.state_cd(+) and
       sitefile.state_cd     = fips.state_cd(+)';

      cleanup(2) := 'drop table FA_STATION' || suffix || ' cascade constraints purge';
   exception
      when others then
         message := 'FAIL to create FA_STATION: ' || SQLERRM;
         dbms_output.put_line(message);
   end create_station;


   procedure create_summaries
   is
   begin

      dbms_output.put_line('creating nwis_station_sum...');

      execute immediate     /* have seen problems with parallel 4, so make it parallel 1 */
     'create table NWIS_STATION_SUM' || suffix || ' compress pctfree 0 nologging parallel 1 as
      select /*+ full(a) parallel(a, 4) */
         pk_isn,
         station_id,
         station_name,
         geom,
         country_cd,
         state_cd,
         county_cd,
         primary_site_type,
         description_text,
         organization_id,
         organization_name,
         nat_aqfr_name,
         aqfr_type_name,
         hydrologic_unit_code,
         well_depth_ft_blw_land_sfc_va,
         nvl2(well_depth_ft_blw_land_sfc_va, ''ft'', null) well_depth_ft_blw_land_sfc_un,
         cast(nvl(b.result_count, 0) as number(8)) result_count
      from
         fa_station' || suffix || ' a
         left join (select fk_station, count(*) result_count from fa_regular_result' || suffix || ' group by fk_station) b
           on a.pk_isn = b.fk_station 
      order by
         country_cd,
         state_cd,
         county_cd,
         primary_site_type';

      cleanup(3) := 'drop table NWIS_STATION_SUM' || suffix || ' cascade constraints purge';

      dbms_output.put_line('creating nwis_result_sum...');

      execute immediate
     'create table NWIS_RESULT_SUM' || suffix || ' compress pctfree 0 nologging noparallel
      partition by range(activity_start_date_time)
         (
            partition nwis_result_sum_pre_1990 values less than (to_date(''01-JAN-1990'', ''DD-MON-YYYY'')),
            partition nwis_result_sum_1990     values less than (to_date(''01-JAN-1991'', ''DD-MON-YYYY'')),
            partition nwis_result_sum_1991     values less than (to_date(''01-JAN-1992'', ''DD-MON-YYYY'')),
            partition nwis_result_sum_1992     values less than (to_date(''01-JAN-1993'', ''DD-MON-YYYY'')),
            partition nwis_result_sum_1993     values less than (to_date(''01-JAN-1994'', ''DD-MON-YYYY'')),
            partition nwis_result_sum_1994     values less than (to_date(''01-JAN-1995'', ''DD-MON-YYYY'')),
            partition nwis_result_sum_1995     values less than (to_date(''01-JAN-1996'', ''DD-MON-YYYY'')),
            partition nwis_result_sum_1996     values less than (to_date(''01-JAN-1997'', ''DD-MON-YYYY'')),
            partition nwis_result_sum_1997     values less than (to_date(''01-JAN-1998'', ''DD-MON-YYYY'')),
            partition nwis_result_sum_1998     values less than (to_date(''01-JAN-1999'', ''DD-MON-YYYY'')),
            partition nwis_result_sum_1999     values less than (to_date(''01-JAN-2000'', ''DD-MON-YYYY'')),
            partition nwis_result_sum_2000     values less than (to_date(''01-JAN-2001'', ''DD-MON-YYYY'')),
            partition nwis_result_sum_2001     values less than (to_date(''01-JAN-2002'', ''DD-MON-YYYY'')),
            partition nwis_result_sum_2002     values less than (to_date(''01-JAN-2003'', ''DD-MON-YYYY'')),
            partition nwis_result_sum_2003     values less than (to_date(''01-JAN-2004'', ''DD-MON-YYYY'')),
            partition nwis_result_sum_2004     values less than (to_date(''01-JAN-2005'', ''DD-MON-YYYY'')),
            partition nwis_result_sum_2005     values less than (to_date(''01-JAN-2006'', ''DD-MON-YYYY'')),
            partition nwis_result_sum_2006     values less than (to_date(''01-JAN-2007'', ''DD-MON-YYYY'')),
            partition nwis_result_sum_2007     values less than (to_date(''01-JAN-2008'', ''DD-MON-YYYY'')),
            partition nwis_result_sum_2008     values less than (to_date(''01-JAN-2009'', ''DD-MON-YYYY'')),
            partition nwis_result_sum_2009     values less than (to_date(''01-JAN-2010'', ''DD-MON-YYYY'')),
            partition nwis_result_sum_2010     values less than (to_date(''01-JAN-2011'', ''DD-MON-YYYY'')),
            partition nwis_result_sum_2011     values less than (to_date(''01-JAN-2012'', ''DD-MON-YYYY'')),
            partition nwis_result_sum_last     values less than (maxvalue)
         )
         as
         select /*+ full(a) parallel(a, 4) full(b) parallel(b, 4) use_hash(a) use_hash(b) */
            fk_station,
            a.station_id,
            country_cd,
            state_cd,
            county_cd,
            primary_site_type,
            organization_id,
            hydrologic_unit_code,
            activity_media_name,
            characteristic_type,
            characteristic_name,
            activity_start_date_time,
            parameter_code,
            b.nemi_url,
            b.result_count
         from
             nwis_station_sum' || suffix || ' a,
             (select
                 fk_station, activity_media_name, characteristic_type, characteristic_name, parameter_code,
                 trunc(activity_start_date_time) activity_start_date_time, nemi_url,
                 cast(count(*) as number(9)) result_count
              from
                 fa_regular_result' || suffix || '
              group by
                 fk_station, activity_media_name, characteristic_type, characteristic_name, parameter_code, nemi_url,
                 trunc(activity_start_date_time)
             ) b
         where
             b.fk_station = a.pk_isn(+)
         order by
            fk_station,
            a.station_id,
            activity_media_name,
            characteristic_type,
            characteristic_name,
            parameter_code,
            nemi_url ';

      cleanup(4) := 'drop table NWIS_RESULT_SUM' || suffix || ' cascade constraints purge';

      dbms_output.put_line('creating nwis_result_ct_sum...');

      execute immediate
     'create table NWIS_RESULT_CT_SUM' || suffix || ' pctfree 0 compress nologging noparallel
      partition by list(characteristic_type)
      (
         partition nwis_result_ct_sum_sediment    values (''Sediment''),
         partition nwis_result_ct_sum_organics    values (''Organics, PCBs''),
         partition nwis_result_ct_sum_nutient     values (''Nutrient''),
         partition nwis_result_ct_sum_inorganics1 values (''Inorganics, Minor, Non-metals''),
         partition nwis_result_ct_sum_inorcanics2 values (''Inorganics, Major, Non-metals''),
         partition nwis_result_ct_sum_information values (''Information''),
         partition nwis_result_ct_sum_organics2   values (''Organics, Pesticide''),
         partition nwis_result_ct_sum_micro       values (''Microbiological''),
         partition nwis_result_ct_sum_physical    values (''Physical''),
         partition nwis_result_ct_sum_radio       values (''Radiochemical''),
         partition nwis_result_ct_sum_inorganics3 values (''Inorganics, Major, Metals''),
         partition nwis_result_ct_sum_isotopes    values (''Stable Isotopes''),
         partition nwis_result_ct_sum_inorganics4 values (''Inorganics, Minor, Metals''),
         partition nwis_result_ct_sum_biological  values (''Biological''),
         partition nwis_result_ct_sum_organics3   values (''Organics, Other''),
         partition nwis_result_ct_sum_default     values (default)
      )
      as
      select /*+ full(b) parallel(b, 4) */
         fk_station,
         station_id,
         country_cd,
         state_cd,
         county_cd,
         primary_site_type,
         organization_id,
         hydrologic_unit_code,
         activity_media_name,
         characteristic_type,
         characteristic_name,
         parameter_code,
         nemi_url,
         cast(sum(result_count) as number(9)) result_count
      from
          nwis_result_sum' || suffix || ' a
      group by
         fk_station,
         station_id,
         country_cd,
         state_cd,
         county_cd,
         primary_site_type,
         organization_id,
         hydrologic_unit_code,
         activity_media_name,
         characteristic_type,
         characteristic_name,
         parameter_code,
         nemi_url
      order by
         fk_station,
         station_id,
         activity_media_name,
         characteristic_type,
         characteristic_name,
         parameter_code,
         nemi_url ';

      cleanup(5) := 'drop table NWIS_RESULT_CT_SUM' || suffix || ' cascade constraints purge';

      dbms_output.put_line('creating nwis_result_nr_sum...');

      execute immediate
     'create table NWIS_RESULT_NR_SUM' || suffix || ' pctfree 0 compress nologging noparallel
      partition by range(activity_start_date_time)
      (
         partition nwis_result_nr_sum_pre_1990 values less than (to_date(''01-JAN-1990'', ''DD-MON-YYYY'')),
         partition nwis_result_nr_sum_1990     values less than (to_date(''01-JAN-1991'', ''DD-MON-YYYY'')),
         partition nwis_result_nr_sum_1991     values less than (to_date(''01-JAN-1992'', ''DD-MON-YYYY'')),
         partition nwis_result_nr_sum_1992     values less than (to_date(''01-JAN-1993'', ''DD-MON-YYYY'')),
         partition nwis_result_nr_sum_1993     values less than (to_date(''01-JAN-1994'', ''DD-MON-YYYY'')),
         partition nwis_result_nr_sum_1994     values less than (to_date(''01-JAN-1995'', ''DD-MON-YYYY'')),
         partition nwis_result_nr_sum_1995     values less than (to_date(''01-JAN-1996'', ''DD-MON-YYYY'')),
         partition nwis_result_nr_sum_1996     values less than (to_date(''01-JAN-1997'', ''DD-MON-YYYY'')),
         partition nwis_result_nr_sum_1997     values less than (to_date(''01-JAN-1998'', ''DD-MON-YYYY'')),
         partition nwis_result_nr_sum_1998     values less than (to_date(''01-JAN-1999'', ''DD-MON-YYYY'')),
         partition nwis_result_nr_sum_1999     values less than (to_date(''01-JAN-2000'', ''DD-MON-YYYY'')),
         partition nwis_result_nr_sum_2000     values less than (to_date(''01-JAN-2001'', ''DD-MON-YYYY'')),
         partition nwis_result_nr_sum_2001     values less than (to_date(''01-JAN-2002'', ''DD-MON-YYYY'')),
         partition nwis_result_nr_sum_2002     values less than (to_date(''01-JAN-2003'', ''DD-MON-YYYY'')),
         partition nwis_result_nr_sum_2003     values less than (to_date(''01-JAN-2004'', ''DD-MON-YYYY'')),
         partition nwis_result_nr_sum_2004     values less than (to_date(''01-JAN-2005'', ''DD-MON-YYYY'')),
         partition nwis_result_nr_sum_2005     values less than (to_date(''01-JAN-2006'', ''DD-MON-YYYY'')),
         partition nwis_result_nr_sum_2006     values less than (to_date(''01-JAN-2007'', ''DD-MON-YYYY'')),
         partition nwis_result_nr_sum_2007     values less than (to_date(''01-JAN-2008'', ''DD-MON-YYYY'')),
         partition nwis_result_nr_sum_2008     values less than (to_date(''01-JAN-2009'', ''DD-MON-YYYY'')),
         partition nwis_result_nr_sum_2009     values less than (to_date(''01-JAN-2010'', ''DD-MON-YYYY'')),
         partition nwis_result_nr_sum_2010     values less than (to_date(''01-JAN-2011'', ''DD-MON-YYYY'')),
         partition nwis_result_nr_sum_2011     values less than (to_date(''01-JAN-2012'', ''DD-MON-YYYY'')),
         partition nwis_result_nr_sum_last     values less than (maxvalue)
      )
      as
      select /*+ full(b) parallel(b, 4) */
         fk_station,
         activity_media_name,
         characteristic_type,
         characteristic_name,
         parameter_code,
         activity_start_date_time,
         nemi_url,
         cast(sum(result_count) as number(9)) result_count
      from
          nwis_result_sum' || suffix || ' a
      group by
         fk_station,
         activity_media_name,
         characteristic_type,
         characteristic_name,
         parameter_code,
         activity_start_date_time,
         nemi_url
      order by
         fk_station,
         activity_media_name,
         characteristic_type,
         characteristic_name,
         parameter_code,
         activity_start_date_time,
         nemi_url ';

      cleanup(6) := 'drop table NWIS_RESULT_NR_SUM' || suffix || ' cascade constraints purge';

      dbms_output.put_line('creating nwis_lctn_loc...');

      execute immediate
     'create table nwis_lctn_loc' || suffix || ' compress pctfree 0 nologging noparallel as
      select /*+ parallel(4) */ distinct
             country_cd,
             state_cd state_fips,
             organization_id,
             organization_name
        from fa_station' || suffix;

      cleanup(7) := 'drop table nwis_lctn_loc' || suffix || ' cascade constraints purge';
      
      dbms_output.put_line('creating nwis_di_org...');

      execute immediate
     'create table nwis_di_org' || suffix || ' compress pctfree 0 nologging noparallel as
      select distinct
             cast(''USGS-'' || state_postal_cd as varchar2(7)) as organization_id,
             ''USGS '' || STATE_NAME || '' Water Science Center'' as organization_name
        from nwis_district_cds_by_host';

      cleanup(8) := 'drop table nwis_di_org' || suffix || ' cascade constraints purge';

   exception
      when others then
         message := 'FAIL to create summaries: ' || SQLERRM;
         dbms_output.put_line(message);
   end create_summaries;

   procedure create_series_catalog
   is
   begin

      dbms_output.put_line('creating series catalog...');

      execute immediate
     'create table SERIES_CATALOG' || suffix || ' compress pctfree 0 nologging as
      select
         c.site_id       AS fk_station,
         c.data_type_cd  AS data_type_code,
         c.parm_cd       AS parameter_code,
         c.stat_cd       AS status_code,
         c.loc_nm        AS location_name,
         c.medium_grp_cd AS medium_group_code,
         c.parm_grp_cd   AS parameter_group_code,
         c.srs_id,
         c.access_cd     AS access_code,
         c.begin_date    AS begin_date_text,
         c.end_date      AS end_date_text,
         c.count_nu,
         c.series_catalog_md
      from
         temp_series_catalog c,
         fa_station' || suffix || ' s,
         lu_parm p
      where
         c.site_id = s.pk_isn and
         c.parm_cd = p.parm_cd';

      cleanup(9) := 'drop table SERIES_CATALOG' || suffix || ' cascade constraints purge';

   exception
      when others then
         message := 'FAIL to create SERIES_CATALOG: ' || SQLERRM;
         dbms_output.put_line(message);
   end create_series_catalog;

   procedure create_public_srsnames
   is
   begin

      dbms_output.put_line('creating public_srsnames...');

      execute immediate
     'create table public_srsnames' || suffix || ' compress pctfree 0 nologging as
      select lu_parm.parm_cd,
             lu_parm.parm_ds description,
             lu_parm_alias.parm_alias_nm characteristicname,
             lu_parm.parm_unt_tx measureunitcode,
             lu_parm.parm_frac_tx resultsamplefraction,
             lu_parm.parm_temp_tx resulttemperaturebasis,
             lu_parm.parm_stat_tx resultstatisticalbasis,
             lu_parm.parm_tm_tx resulttimebasis,
             lu_parm.parm_wt_tx resultweightbasis,
             lu_parm.parm_size_tx resultparticlesizebasis,
             case
               when lu_parm.parm_rev_dt > lu_parm_alias.parm_alias_rev_dt
                 then lu_parm.parm_rev_dt
               else lu_parm_alias.parm_alias_rev_dt
             end last_rev_dt,
             max(case
                   when lu_parm.parm_rev_dt > lu_parm_alias.parm_alias_rev_dt
                     then lu_parm.parm_rev_dt
                   else lu_parm_alias.parm_alias_rev_dt
                 end) over () max_last_rev_dt
        from lu_parm
             join lu_parm_alias
               on lu_parm.parm_cd = lu_parm_alias.parm_cd and
                  ''SRSNAME'' = lu_parm_alias.parm_alias_cd
       where parm_public_fg = ''Y''
         order by 1';

      cleanup(10) := 'drop table public_srsnames' || suffix || ' cascade constraints purge';

   exception
      when others then
         message := 'FAIL to create public_srsnames: ' || SQLERRM;
         dbms_output.put_line(message);
   end create_public_srsnames;

   procedure create_code_tables
   is
   begin

      dbms_output.put_line('creating characteristicname...');
      execute immediate
      'create table characteristicname' || suffix || ' compress pctfree 0 nologging as
      select cast(code_value as varchar2(500 char)) code_value,
             cast(null as varchar2(4000 char)) description,
             rownum sort_order
        from (select distinct characteristic_name code_value
                from fa_regular_result' || suffix || '
                  order by 1)';
      cleanup(11) := 'drop table characteristicname' || suffix || ' cascade constraints purge';

      dbms_output.put_line('creating characteristictype...');
      execute immediate
      'create table characteristictype' || suffix || ' compress pctfree 0 nologging as
      select cast(code_value as varchar2(500 char)) code_value,
             cast(null as varchar2(4000 char)) description,
             rownum sort_order
        from (select distinct characteristic_type code_value
                from fa_regular_result' || suffix || '
                  order by 1)';
      cleanup(12) := 'drop table characteristictype' || suffix || ' cascade constraints purge';

      dbms_output.put_line('creating country...');
      execute immediate
      'create table country' || suffix || ' compress pctfree 0 nologging as
      select cast(code_value as varchar2(2 char)) code_value,
             cast(description as varchar2(48 char)) description,
             rownum sort_order
        from (select distinct country_cd code_value,
                     country_name description
                from fa_station' || suffix || '
                  order by country_name desc)';
      cleanup(13) := 'drop table country' || suffix || ' cascade constraints purge';

      dbms_output.put_line('creating county...');
      execute immediate
      'create table county' || suffix || ' compress pctfree 0 nologging as
      select cast(code_value as varchar2(9 char)) code_value,
             cast(description as varchar2(107 char)) description,
             cast(country_cd as varchar2(2 char)) country_cd,
             cast(state_cd as varchar2(2 char)) state_cd,
             rownum sort_order
        from (select distinct country_cd||'':''||state_cd|| '':''||county_cd code_value,
                     country_cd||'', ''||state_name||'', ''||county_name description,
                     country_cd,
                     state_cd,
                     county_cd
                from fa_station' || suffix || '
                  order by country_cd desc,
                           state_cd,
                           county_cd)';
     cleanup(14) := 'drop table county' || suffix || ' cascade constraints purge';

      dbms_output.put_line('creating organization...');
      execute immediate
      'create table organization' || suffix || ' compress pctfree 0 nologging as
      select cast(code_value as varchar2(500 char)) code_value,
             cast(description as varchar2(4000 char)) description,
             rownum sort_order
        from (select distinct organization_id code_value, organization_name description
                from fa_station' || suffix || '
                  order by 1)';
      cleanup(15) := 'drop table organization' || suffix || ' cascade constraints purge';

      dbms_output.put_line('creating samplemedia...');
      execute immediate
      'create table samplemedia' || suffix || ' compress pctfree 0 nologging as
       select cast(code_value as varchar2(30 char)) code_value,
              cast(null as varchar2(4000 char)) description,
              rownum sort_order
         from (select distinct activity_media_name code_value
                 from fa_regular_result' || suffix || '
                   order by 1)';
      cleanup(16) := 'drop table samplemedia' || suffix || ' cascade constraints purge';

      dbms_output.put_line('creating sitetype...');
      execute immediate
      'create table sitetype' || suffix || ' compress pctfree 0 nologging as
       select cast(code_value as varchar2(500 char)) code_value,
              cast(null as varchar2(4000 char)) description,
              rownum sort_order
         from (select distinct primary_site_type code_value
                 from fa_station' || suffix || '
                    order by 1)';
      cleanup(17) := 'drop table sitetype' || suffix || ' cascade constraints purge';

      dbms_output.put_line('creating state...');
      execute immediate
      'create table state' || suffix || ' compress pctfree 0 nologging as
      select cast(code_value as varchar2(5 char)) code_value,
             cast(description_with_country as varchar2(57 char)) description_with_country,
             cast(description_with_out_country as varchar2(53 char)) description_with_out_country,
             cast(country_cd as varchar2(2 char)) country_cd,
             rownum sort_order
        from (select distinct country_cd||'':''||state_cd code_value,
                     country_cd||'', ''||state_name description_with_country,
                     state_name description_with_out_country,
                     country_cd,
                     state_cd
                from fa_station' || suffix || '
                  order by country_cd desc,
                           state_cd)';
      cleanup(18) := 'drop table state' || suffix || ' cascade constraints purge';

      exception
      when others then
         message := 'FAIL to create code_tables: ' || SQLERRM;
         dbms_output.put_line(message);
   end create_code_tables;
   
   procedure create_index
   is
      stmt            varchar2(32000);
      table_name      varchar2(   80);
   begin

      dbms_output.put_line('creating indexes...');

      table_name := 'FA_STATION' || suffix;

      stmt := 'create table qwportal_summary' || suffix || ' compress nologging pctfree 0 cache as
               select
                  trim(station.state_cd) as fips_state_code,
                  trim(station.county_cd) as fips_county_code,
                  trim(station.state_cd) || trim(station.county_cd) as fips_state_and_county,
                  cast(''N'' as varchar2(1 char)) as nwis_or_epa,
                  /* took out trim(result.characteristic_type) as characteristic_type, */
                  cast(primary_site_type as varchar2(30)) as site_type,
                  cast(trim(station.hydrologic_unit_code) as varchar2(8)) as huc8,
                  cast(null as varchar2(12)) as huc12,
                  min(case when activity_start_date_time between to_date(''01-JAN-1875'', ''DD-MON-YYYY'') and sysdate then activity_start_date_time else null end) as min_date,
                  max(case when activity_start_date_time between to_date(''01-JAN-1875'', ''DD-MON-YYYY'') and sysdate then activity_start_date_time else null end) as max_date,
                  cast(count(distinct case when months_between(sysdate, activity_start_date_time) between 0 and 12 then sample_id else null end) as number(7)) as samples_past_12_months,
                  cast(count(distinct case when months_between(sysdate, activity_start_date_time) between 0 and 60 then sample_id else null end) as number(7)) as samples_past_60_months,
                  cast(count(distinct result.activity_id) as number(7)) samples_all_time,
                  cast(sum(case when months_between(sysdate, activity_start_date_time) between 0 and 12 then 1 else 0 end) as number(7)) as results_past_12_months,
                  cast(sum(case when months_between(sysdate, activity_start_date_time) between 0 and 60 then 1 else 0 end) as number(7)) as results_past_60_months,
                  cast(count(*) as number(7)) as results_all_time
               from
                  nwis_ws_star.fa_station' || suffix || ' station,
                  nwis_ws_star.fa_regular_result' || suffix || ' result
               where
                  station.pk_isn = result.fk_station and
                  station.state_cd between ''01'' and ''56'' and
                  length(trim(station.state_cd)) = 2
               group by
                  trim(station.state_cd),
                  trim(station.county_cd),
                  trim(station.state_cd) || trim(station.county_cd),
                  /*  took out trim(result.characteristic_type), */
                  primary_site_type,
                  trim(station.hydrologic_unit_code)
               union all
               select
                  fips_state_code,
                  fips_county_code,
                  fips_state_code || fips_county_code fips_state_and_county,
                  cast(''E'' as varchar2(1)) nwis_or_epa,
                  site_type,
                  huc8,
                  huc12,
                  min_date,
                  max_date,
                  samples_past_12_months,
                  samples_past_60_months,
                  samples_all_time,
                  results_past_12_months,
                  results_past_60_months,
                  results_all_time
               from
                  storetmodern.storet_sum';

      cleanup(19) := 'drop table qwportal_summary' || suffix || ' cascade constraints purge';

      dbms_output.put_line('creating qwportal_summary...');
      execute immediate stmt;

      stmt := 'alter table ' || table_name || ' cache noparallel';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'alter table ' || table_name || ' add constraint pk_' || table_name || ' primary key (pk_isn) using index nologging';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index fa_station_primary_type' || suffix || ' on ' ||
               table_name || ' (primary_site_type) nologging';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index fa_station_co_st_co' || suffix || ' on ' ||
               table_name || ' (country_cd, state_cd, county_cd) nologging';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create index station_id' || suffix || ' on ' ||
               table_name || ' (station_id) nologging';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      table_name := 'FA_REGULAR_RESULT' || suffix;

      /*------------usage:
         making some of these bitmap?  e.g. fk_station.
            activity_media_name
            characteristic_name
            characteristic_type
            activity_id
            parameter_code
      ---------------------------------*/
      stmt := 'alter table ' || table_name || ' cache ';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index fk_station_i' || suffix || ' on ' ||
               table_name || ' (FK_STATION) local nologging ';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index activity_id_i' || suffix || ' on ' ||
               table_name || ' (ACTIVITY_ID) local nologging ';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'alter table ' || table_name || ' add (constraint fk_station_fk' || suffix ||
              ' foreign key (fk_station) references ' || 'fa_station' || suffix || ' (pk_isn))';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index media_nm_bm_i' || suffix || ' on ' ||
               table_name || ' (activity_media_name) local nologging';
      execute immediate stmt;

      stmt := 'create bitmap index char_nm_bm_i' || suffix || ' on ' ||
               table_name || ' (characteristic_name) local nologging';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index parm_i' || suffix || ' on ' ||
               table_name || ' (parameter_code) local nologging';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index char_type_bm_i' || suffix || ' on ' ||
               table_name || ' (characteristic_type) local nologging';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index nemi_url_i' || suffix || ' on ' ||
               table_name || ' (nemi_url) local nologging';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      /* note: this view seems to use the invoker rather than the definer.
               so, must run as NWIS_WS_STAR, despite the fact that everything
               else in this large package would work as any user with rights
               to execute the package */
      delete from user_sdo_geom_metadata where table_name = 'FA_STATION' || suffix;
      insert INTO USER_SDO_GEOM_METADATA VALUES('FA_STATION' || suffix, 'GEOM',
                  MDSYS.SDO_DIM_ARRAY( MDSYS.SDO_DIM_ELEMENT('X', -180, 180, 0.005), MDSYS.SDO_DIM_ELEMENT('Y', -90, 90, 0.005)), 8265);
      commit work;

      stmt := 'create index fa_station_geom_sp_idx' || suffix || ' on ' ||
              'FA_STATION' || suffix || ' (GEOM) INDEXTYPE IS MDSYS.SPATIAL_INDEX PARAMETERS (''SDO_INDX_DIMS=2 LAYER_GTYPE="POINT"'')';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      table_name := 'SERIES_CATALOG' || suffix;

      stmt := 'create index fk_station2_i' || suffix || ' on ' ||
               table_name || ' (FK_STATION) nologging compress';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'alter table ' || table_name || ' add (constraint fk_station2_fk' || suffix ||
              ' foreign key (fk_station) references ' || 'fa_station' || suffix || ' (pk_isn))';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      table_name := 'NWIS_STATION_SUM' || suffix;
      stmt := 'create bitmap index nwis_station_sum_1' || suffix || ' on ' ||
               table_name || ' (station_id          ) nologging';

      delete from user_sdo_geom_metadata where table_name = 'NWIS_STATION_SUM' || suffix;
      insert INTO USER_SDO_GEOM_METADATA VALUES('NWIS_STATION_SUM' || suffix, 'GEOM',
                  MDSYS.SDO_DIM_ARRAY( MDSYS.SDO_DIM_ELEMENT('X', -180, 180, 0.005), MDSYS.SDO_DIM_ELEMENT('Y', -90, 90, 0.005)), 8265);
      commit work;

      stmt := 'create        index nwis_station_sum_2' || suffix || ' on ' ||
               table_name || ' (geom) INDEXTYPE IS MDSYS.SPATIAL_INDEX PARAMETERS (''SDO_INDX_DIMS=2 LAYER_GTYPE="POINT"'')';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index nwis_station_sum_3' || suffix || ' on ' ||
               table_name || ' (state_cd, county_cd ) nologging';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index nwis_station_sum_4' || suffix || ' on ' ||
               table_name || ' (primary_site_type) nologging';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index nwis_station_sum_5' || suffix || ' on ' ||
               table_name || ' (organization_id     ) nologging';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index nwis_station_sum_6' || suffix || ' on ' ||
               table_name || ' (hydrologic_unit_code) nologging';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      table_name := 'NWIS_RESULT_SUM' || suffix;

      stmt := 'create bitmap index nwis_result_sum_1' || suffix || ' on ' ||
               table_name || ' (fk_station          ) local nologging';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index nwis_result_sum_2' || suffix || ' on ' ||
               table_name || ' (state_cd, county_cd ) local nologging';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index nwis_result_sum_3' || suffix || ' on ' ||
               table_name || ' (primary_site_type) local nologging';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index nwis_result_sum_4' || suffix || ' on ' ||
               table_name || ' (organization_id     ) local nologging';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index nwis_result_sum_5' || suffix || ' on ' ||
               table_name || ' (hydrologic_unit_code) local nologging';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index nwis_result_sum_6' || suffix || ' on ' ||
               table_name || ' (activity_media_name ) local nologging';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index nwis_result_sum_7' || suffix || ' on ' ||
               table_name || ' (characteristic_type ) local nologging';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index nwis_result_sum_8' || suffix || ' on ' ||
               table_name || ' (characteristic_name ) local nologging';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index nwis_result_sum_9' || suffix || ' on ' ||
               table_name || ' (parameter_code      ) local nologging';
      dbms_output.put_line(stmt);
      execute immediate stmt;
      
      stmt := 'create bitmap index nwis_result_sum_10' || suffix || ' on ' ||
               table_name || ' (nemi_url) local nologging';
      dbms_output.put_line(stmt);
      execute immediate stmt;


      table_name := 'NWIS_RESULT_CT_SUM' || suffix;
      stmt := 'create bitmap index nwis_result_ct_sum_1' || suffix || ' on ' ||
               table_name || ' (fk_station          ) local nologging';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index nwis_result_ct_sum_2' || suffix || ' on ' ||
               table_name || ' (state_cd, county_cd ) local nologging';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index nwis_result_ct_sum_3' || suffix || ' on ' ||
               table_name || ' (primary_site_type) local nologging';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index nwis_result_ct_sum_4' || suffix || ' on ' ||
               table_name || ' (organization_id     ) local nologging';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index nwis_result_ct_sum_5' || suffix || ' on ' ||
               table_name || ' (hydrologic_unit_code) local nologging';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index nwis_result_ct_sum_6' || suffix || ' on ' ||
               table_name || ' (activity_media_name ) local nologging';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index nwis_result_ct_sum_7' || suffix || ' on ' ||
               table_name || ' (characteristic_type ) local nologging';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index nwis_result_ct_sum_8' || suffix || ' on ' ||
               table_name || ' (characteristic_name ) local nologging';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index nwis_result_ct_sum_9' || suffix || ' on ' ||
               table_name || ' (nemi_url) local nologging';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      table_name := 'NWIS_RESULT_NR_SUM' || suffix;
      stmt := 'create bitmap index nwis_result_nr_sum_1' || suffix || ' on ' ||
               table_name || ' (fk_station          ) local nologging';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index nwis_result_nr_sum_2' || suffix || ' on ' ||
               table_name || ' (activity_media_name ) local nologging';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index nwis_result_nr_sum_3' || suffix || ' on ' ||
               table_name || ' (characteristic_type ) local nologging';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index nwis_result_nr_sum_4' || suffix || ' on ' ||
               table_name || ' (characteristic_name ) local nologging';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index nwis_result_nr_sum_5' || suffix || ' on ' ||
               table_name || ' (nemi_url) local nologging';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      dbms_output.put_line('grants...');
      execute immediate 'grant select on fa_station'          || suffix || ' to nwis_ws_user';
      execute immediate 'grant select on fa_regular_result'   || suffix || ' to nwis_ws_user, wqp_user';
      execute immediate 'grant select on series_catalog'      || suffix || ' to nwis_ws_user';
      execute immediate 'grant select on qwportal_summary'    || suffix || ' to nwis_ws_user';
      execute immediate 'grant select on nwis_station_sum'    || suffix || ' to nwis_ws_user';
      execute immediate 'grant select on nwis_result_sum'     || suffix || ' to nwis_ws_user';
      execute immediate 'grant select on nwis_result_ct_sum'  || suffix || ' to nwis_ws_user';
      execute immediate 'grant select on nwis_result_nr_sum'  || suffix || ' to nwis_ws_user';
      execute immediate 'grant select on nwis_lctn_loc'       || suffix || ' to nwis_ws_user';
      execute immediate 'grant select on nwis_di_org'         || suffix || ' to nwis_ws_user';
      execute immediate 'grant select on public_srsnames'     || suffix || ' to nwis_ws_user';
      execute immediate 'grant select on characteristicname'  || suffix || ' to nwis_ws_user';
      execute immediate 'grant select on characteristictype'  || suffix || ' to nwis_ws_user';
      execute immediate 'grant select on country'             || suffix || ' to nwis_ws_user, ars_stewards';
      execute immediate 'grant select on county'              || suffix || ' to nwis_ws_user, ars_stewards';
      execute immediate 'grant select on organization'        || suffix || ' to nwis_ws_user';
      execute immediate 'grant select on samplemedia'         || suffix || ' to nwis_ws_user';
      execute immediate 'grant select on sitetype'            || suffix || ' to nwis_ws_user';
      execute immediate 'grant select on state'               || suffix || ' to nwis_ws_user, ars_stewards';

      dbms_output.put_line('analyze fa_station...');  /* takes about 1.5 minutes*/
      dbms_stats.gather_table_stats('NWIS_WS_STAR', 'FA_STATION'        || suffix, null, 100, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      dbms_output.put_line('analyze fa_regular_result...');  /* takes about 20 minutes */
      dbms_stats.gather_table_stats('NWIS_WS_STAR', 'FA_REGULAR_RESULT' || suffix, null,  10, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      dbms_output.put_line('analyze series_catalog...');  /* takes about 3 minutes */
      dbms_stats.gather_table_stats('NWIS_WS_STAR', 'SERIES_CATALOG'    || suffix, null, 100, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      dbms_output.put_line('analyze qwportal_summary...');  /* takes about ?? minutes */
      dbms_stats.gather_table_stats('NWIS_WS_STAR', 'QWPORTAL_SUMMARY'  || suffix, null, 100, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      dbms_output.put_line('analyze nwis_station_sum...');  /* takes about ?? minutes */
      dbms_stats.gather_table_stats('NWIS_WS_STAR', 'NWIS_STATION_SUM'  || suffix, null, 100, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      dbms_output.put_line('analyze nwis_result_sum...');  /* takes about ?? minutes */
      dbms_stats.gather_table_stats('NWIS_WS_STAR', 'NWIS_RESULT_SUM'  || suffix, null, 10, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      dbms_output.put_line('analyze nwis_result_ct_sum...');  /* takes about ?? minutes */
      dbms_stats.gather_table_stats('NWIS_WS_STAR', 'NWIS_RESULT_CT_SUM'  || suffix, null, 10, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      dbms_output.put_line('analyze nwis_result_nr_sum...');  /* takes about ?? minutes */
      dbms_stats.gather_table_stats('NWIS_WS_STAR', 'NWIS_RESULT_NR_SUM'  || suffix, null, 10, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      dbms_output.put_line('analyze nwis_lctn_loc...');  /* takes about ?? minutes */
      dbms_stats.gather_table_stats('NWIS_WS_STAR', 'NWIS_LCTN_LOC'  || suffix, null, 10, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      dbms_output.put_line('analyze nwis_di_org...');  /* takes about ?? minutes */
      dbms_stats.gather_table_stats('NWIS_WS_STAR', 'NWIS_DI_ORG'  || suffix, null, 10, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      dbms_output.put_line('analyze public_srsnames...');  /* takes about ?? minutes */
      dbms_stats.gather_table_stats('NWIS_WS_STAR', 'PUBLIC_SRSNAMES'  || suffix, null, 10, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);

      dbms_output.put_line('analyze characteristicname...');  /* takes about ?? minutes */
      dbms_stats.gather_table_stats('NWIS_WS_STAR', 'CHARACTERISTICNAME'  || suffix, null, 10, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      dbms_output.put_line('analyze characteristictype...');  /* takes about ?? minutes */
      dbms_stats.gather_table_stats('NWIS_WS_STAR', 'CHARACTERISTICTYPE'  || suffix, null, 10, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      dbms_output.put_line('analyze country...');  /* takes about ?? minutes */
      dbms_stats.gather_table_stats('NWIS_WS_STAR', 'COUNTRY'  || suffix, null, 10, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      dbms_output.put_line('analyze county...');  /* takes about ?? minutes */
      dbms_stats.gather_table_stats('NWIS_WS_STAR', 'COUNTY'  || suffix, null, 10, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      dbms_output.put_line('analyze organization...');  /* takes about ?? minutes */
      dbms_stats.gather_table_stats('NWIS_WS_STAR', 'ORGANIZATION'  || suffix, null, 10, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      dbms_output.put_line('analyze samplemedia...');  /* takes about ?? minutes */
      dbms_stats.gather_table_stats('NWIS_WS_STAR', 'SAMPLEMEDIA'  || suffix, null, 10, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      dbms_output.put_line('analyze sitetype...');  /* takes about ?? minutes */
      dbms_stats.gather_table_stats('NWIS_WS_STAR', 'SITETYPE'  || suffix, null, 10, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      dbms_output.put_line('analyze state...');  /* takes about ?? minutes */
      dbms_stats.gather_table_stats('NWIS_WS_STAR', 'STATE'  || suffix, null, 10, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);

   exception
      when others then
         message := 'FAIL with index: ' || stmt || '  --> ' || SQLERRM;
         dbms_output.put_line(message);
   end create_index;

   procedure validate
   is
      old_rows     int;
      new_rows     int;
      index_count  int;
      grant_count  int;
      type cursor_type is ref cursor;
      c            cursor_type;
      query        varchar2(4000);
      pass_fail    varchar2(15);
      situation    varchar2(200);
   begin

      dbms_output.put_line('validating...');

      select count(*) into old_rows from fa_regular_result;
      query := 'select count(*) from fa_regular_result' || suffix;
      open c for query;
      fetch c into new_rows;
      close c;

      if new_rows > 70000000 and new_rows > old_rows - 9000000 then
         pass_fail := 'PASS';
      else
         pass_fail := 'FAIL';
      	 $IF $$empty_db $THEN
      	    pass_fail := 'PASS empty_db';
      	 $END
      end if;
      situation := pass_fail || ': table comparison for fa_regular_result: was ' || trim(to_char(old_rows, '999,999,999')) || ', now ' || trim(to_char(new_rows, '999,999,999'));
      dbms_output.put_line(situation);
      if pass_fail = 'FAIL' and message is null then
         message := situation;
      end if;

      select count(*) into old_rows from fa_station;
      query := 'select count(*) from fa_station' || suffix;
      open c for query;
      fetch c into new_rows;
      close c;

      if new_rows > 1400000 and new_rows > old_rows - 100000 then
         pass_fail := 'PASS';
      else
         pass_fail := 'FAIL';
      	 $IF $$empty_db $THEN
      	    pass_fail := 'PASS empty_db';
      	 $END
      end if;
      situation := pass_fail || ': table comparison for fa_station: was ' || trim(to_char(old_rows, '999,999,999')) || ', now ' || trim(to_char(new_rows, '999,999,999'));
      dbms_output.put_line(situation);
      if pass_fail = 'FAIL' and message is null then
         message := situation;
      end if;

      select count(*) into old_rows from series_catalog;
      query := 'select count(*) from series_catalog' || suffix;
      open c for query;
      fetch c into new_rows;
      close c;

      if new_rows > 14000000 and new_rows > old_rows - 2000000 then
         pass_fail := 'PASS';
      else
         pass_fail := 'FAIL';
      	 $IF $$empty_db $THEN
      	    pass_fail := 'PASS empty_db';
      	 $END
      end if;
      situation := pass_fail || ': table comparison for series_catalog: was ' || trim(to_char(old_rows, '999,999,999')) || ', now ' || trim(to_char(new_rows, '999,999,999'));
      dbms_output.put_line(situation);
      if pass_fail = 'FAIL' and message is null then
         message := situation;
      end if;

      select count(*) into old_rows from qwportal_summary;
      query := 'select count(*) from qwportal_summary' || suffix;
      open c for query;
      fetch c into new_rows;
      close c;

      if new_rows > 20000 then
         pass_fail := 'PASS';
      else
         pass_fail := 'FAIL';
      	 $IF $$empty_db $THEN
      	    pass_fail := 'PASS empty_db';
      	 $END
      end if;
      situation := pass_fail || ': table comparison for qwportal_summary: was ' || trim(to_char(old_rows, '999,999,999')) || ', now ' || trim(to_char(new_rows, '999,999,999'));
      dbms_output.put_line(situation);
      if pass_fail = 'FAIL' and message is null then
         message := situation;
      end if;

      query := 'select count(*) from user_indexes where ' || table_list || ' and substr(table_name, -5) = substr(:current_suffix, 2)';
      open  c for query using suffix;
      fetch c into index_count;
      close c;

      if index_count < 46 then  /* there are exactly 46 as of 10MAR2014 */
         pass_fail := 'FAIL';
      else
         pass_fail := 'PASS';
      end if;
      situation := pass_fail || ': found ' || to_char(index_count) || ' indexes.';
      dbms_output.put_line(situation);
      if pass_fail = 'FAIL' and message is null then
         message := situation;
      end if;

      query := 'select count(*) from user_tab_privs where grantee = ''NWIS_WS_USER'' and ' || table_list ||
               ' and substr(table_name, -5) = substr(:current_suffix, 2)';
      open  c for query using suffix;
      fetch c into grant_count;
      close c;

      if grant_count < 19 then  /* there are exactly 19 as of 10MAR2014 */
         pass_fail := 'FAIL';
      else
         pass_fail := 'PASS';
      end if;
      situation := pass_fail || ': found ' || to_char(grant_count) || ' grants.';
      dbms_output.put_line(situation);
      if pass_fail = 'FAIL' and message is null then
         message := situation;
      end if;

   exception
      when others then
         message := 'FAIL validation with query problem: ' || query || ' ' || SQLERRM;
         dbms_output.put_line(message);

   end validate;

   procedure install
   is
   	  suffix_less_one varchar2(10) := '_' || to_char(to_number(substr(suffix, 2) - 1), 'fm00000');
   begin

      dbms_output.put_line('installing...');

      execute immediate 'create or replace synonym fa_station for fa_station'                  || suffix;
      execute immediate 'create or replace synonym fa_regular_result for fa_regular_result'    || suffix;
      execute immediate 'create or replace synonym series_catalog for series_catalog'          || suffix;
      execute immediate 'create or replace synonym qwportal_summary for qwportal_summary'      || suffix;
      execute immediate 'create or replace synonym nwis_station_sum for nwis_station_sum'      || suffix;
      execute immediate 'create or replace synonym nwis_result_sum for nwis_result_sum'        || suffix;
      execute immediate 'create or replace synonym nwis_result_ct_sum for nwis_result_ct_sum'  || suffix;
      execute immediate 'create or replace synonym nwis_result_nr_sum for nwis_result_nr_sum'  || suffix;
      execute immediate 'create or replace synonym public_srsnames for public_srsnames'        || suffix;
      execute immediate 'create or replace synonym characteristicname for characteristicname'  || suffix;
      execute immediate 'create or replace synonym characteristictype for characteristictype'  || suffix;
      execute immediate 'create or replace synonym country for country'                        || suffix;
      execute immediate 'create or replace synonym county for county'                          || suffix;
      execute immediate 'create or replace synonym organization for organization'              || suffix;
      execute immediate 'create or replace synonym samplemedia for samplemedia'                || suffix;
      execute immediate 'create or replace synonym sitetype for sitetype'                      || suffix;
      execute immediate 'create or replace synonym state for state'                            || suffix;

      execute immediate 'create or replace synonym nwis_lctn_loc_new for nwis_lctn_loc'        || suffix;
      execute immediate 'create or replace synonym nwis_lctn_loc_old for nwis_lctn_loc'        || suffix_less_one;

      execute immediate 'create or replace synonym nwis_di_org_new for nwis_di_org'            || suffix;
      execute immediate 'create or replace synonym nwis_di_org_old for nwis_di_org'            || suffix_less_one;

   exception
      when others then
         message := 'FAIL with synonyms: ' || SQLERRM;
         dbms_output.put_line(message);

   end install;

   procedure drop_old_stuff
   is
      to_drop cursor_type;
      drop_query varchar2(4000) := 'select table_name from user_tables where ' || table_list ||
            ' and substr(table_name, -5) <= to_char(to_number(substr(:current_suffix, 2) - 2), ''fm00000'')' ||
            ' and substr(table_name, -5) <> ''00000''' ||
               ' order by case when table_name like ''FA_STATION%'' then 2 else 1 end, table_name';

      to_nocache cursor_type;
      nocache_query varchar2(4000) := 'select table_name from user_tables where ' || table_list ||
            ' and substr(table_name, -5) <= to_char(to_number(substr(:current_suffix, 2) - 1), ''fm00000'')' ||
               ' order by case when table_name like ''FA_STATION%'' then 2 else 1 end, table_name';

      drop_name varchar2(30);
      nocache_name varchar2(30);
      stmt      varchar2(80);
   begin

      dbms_output.put_line('drop_old_stuff...');

      open to_drop for drop_query using suffix;
      loop
         fetch to_drop into drop_name;
         exit when to_drop%NOTFOUND;
         stmt := 'drop table ' || drop_name;
         dbms_output.put_line('CLEANUP old stuff: ' || stmt);
         execute immediate stmt;
      end loop;
      close to_drop;

      open to_nocache for nocache_query using suffix;
      loop
         fetch to_nocache into nocache_name;
         exit when to_nocache%NOTFOUND;
         stmt := 'alter table ' || nocache_name || ' nocache';
         dbms_output.put_line('CLEANUP old stuff: ' || stmt);
         execute immediate stmt;
      end loop;
      close to_nocache;

   exception
      when others then
         message := 'tried to drop ' || drop_name || ' : ' || SQLERRM;
         dbms_output.put_line(message);

   end drop_old_stuff;

   procedure main(mesg in out varchar2, success_notify in varchar2, failure_notify in varchar2) is
      email_subject varchar2(  80);
      email_notify  varchar2( 400);
      k int;
   begin
      message := null;
      dbms_output.enable(100000);

      for k in 1 .. 20 loop
         cleanup(k) := NULL;
      end loop;

      dbms_output.put_line('started nad table transformation.');
      determine_suffix;
      if message is null then create_regular_result;  end if;
      if message is null then create_station;         end if;
      if message is null then create_series_catalog;  end if;
      if message is null then create_summaries;       end if;
      if message is null then create_public_srsnames; end if;
      if message is null then create_code_tables;     end if;
      if message is null then create_index;           end if;
      if message is null then validate;               end if;
      if message is null then
         install;
      else
         dbms_output.put_line('completed. (failed)');
         dbms_output.put_line('errors occurred.');
         dbms_output.put_line('nad load FAILED');
         email_notify := failure_notify;
         for k in 1 .. 20 loop
            if cleanup(k) is not null then
               dbms_output.put_line('CLEANUP: ' || cleanup(k));
               execute immediate cleanup(k);
            end if;
         end loop;
      end if;

      if message is null then
         drop_old_stuff;
         if message is null then
            dbms_output.put_line('completed. (success)');
         else
            dbms_output.put_line('completed. (failed)');
            dbms_output.put_line('errors occurred.');
         end if;
      end if;
      
      mesg := message;

   end main;
end create_nad_objects;
/
