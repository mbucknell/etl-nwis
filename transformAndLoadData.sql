show user;
select * from global_name;
set timing on;
set serveroutput on;
whenever sqlerror exit failure rollback;
whenever oserror exit failure rollback;
select 'transform and load start time: ' || systimestamp from dual;

prompt !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
prompt !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
prompt !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
prompt !! TEMPORARY COPY OF DATA FROM PRODUCTION
drop table nwis_fa_station cascade constraints purge;
drop table nwis_fa_result cascade constraints purge;
drop table nwis_qw_result cascade constraints purge;
drop table nwis_qw_sample cascade constraints purge;
drop table nwis_series_catalog cascade constraints purge;
drop table nwis_sitefile cascade constraints purge;
create table nwis_fa_station compress pctfree 0 nologging parallel 4 as select /*+ parallel(4) */ * from fa_station@nwis_ws_star_dbdw;
create table nwis_fa_result compress pctfree 0 nologging parallel 4 as select /*+ parallel(4) */ * from fa_regular_result@nwis_ws_star_dbdw;
create table nwis_qw_result compress pctfree 0 nologging parallel 4 as select /*+ parallel(4) */ * from qw_result@nwis_ws_star_dbdw;
create table nwis_qw_sample compress pctfree 0 nologging parallel 4 as select /*+ parallel(4) */ * from qw_sample@nwis_ws_star_dbdw;
create table nwis_series_catalog compress pctfree 0 nologging parallel 4 as select /*+ parallel(4) */ * from series_catalog@nwis_ws_star_dbdw;
create table nwis_sitefile compress pctfree 0 nologging parallel 4 as select /*+ parallel(4) */ * from sitefile@nwis_ws_star_dbdw;
prompt !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
prompt !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
prompt !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!


prompt ****************************************************************************************************************
prompt ** dropRI.sql 
prompt ****************************************************************************************************************
@dropRI.sql;

prompt ****************************************************************************************************************
prompt ** transformStation.sql 
prompt ****************************************************************************************************************
@transformStation.sql;

prompt ****************************************************************************************************************
prompt ** transformResult.sql
prompt ****************************************************************************************************************
@transformResult.sql;

prompt ****************************************************************************************************************
prompt ** createSummaries.sql
prompt ****************************************************************************************************************
@createSummaries.sql;

prompt ****************************************************************************************************************
prompt ** createCodes.sql
prompt ****************************************************************************************************************
@createCodes.sql;

prompt ****************************************************************************************************************
prompt ** buildIndexes.sql
prompt ****************************************************************************************************************
@buildIndexes.sql;

prompt ****************************************************************************************************************
prompt ** addRI.sql 
prompt ****************************************************************************************************************
@addRI.sql;

prompt ****************************************************************************************************************
prompt ** analyze.sql
prompt ****************************************************************************************************************
@analyze.sql;

prompt ****************************************************************************************************************
prompt ** validate.sql
prompt ****************************************************************************************************************
@validate.sql;

prompt ****************************************************************************************************************
prompt ** install.sql
prompt ****************************************************************************************************************
@install.sql;

prompt ****************************************************************************************************************
prompt ** createPublicSrsnames.sql
prompt ****************************************************************************************************************
@createPublicSrsnames.sql;

select 'transform and load time: ' || systimestamp from dual;
