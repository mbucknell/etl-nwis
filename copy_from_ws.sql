show user;
whenever sqlerror exit failure rollback;
whenever oserror exit failure rollback;
set timing on;
set serveroutput on;
set linesize 160
exec dbms_output.enable(100000);
select 'start time: ' || systimestamp from dual;

truncate table sitefile;
insert /*+ append nologging */into sitefile select * from sitefile@nwis_ws_star_wiws.er.usgs.gov;
commit;

truncate table qw_sample;
insert /*+ append nologging */into sitefile select * from qw_sample@nwis_ws_star_wiws.er.usgs.gov;
commit;

truncate table sitefile;
insert /*+ append nologging */into qw_result select * from qw_result@nwis_ws_star_wiws.er.usgs.gov;
commit;

truncate table temp_series_catalog;
insert /*+ append nologging */into sitefile select * from temp_series_catalog@nwis_ws_star_wiws.er.usgs.gov;
commit;

begin
	dbms_stats.gather_table_stats('NWIS_WS_STAR', 'SITEFILE'  || suffix, null, 10, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
	dbms_stats.gather_table_stats('NWIS_WS_STAR', 'QW_SAMPLE'  || suffix, null, 10, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
	dbms_stats.gather_table_stats('NWIS_WS_STAR', 'QW_RESULT'  || suffix, null, 10, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
	dbms_stats.gather_table_stats('NWIS_WS_STAR', 'TEMP_SERIES_CATALOG'  || suffix, null, 10, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
end;
/

select 'end time: ' || systimestamp from dual;
