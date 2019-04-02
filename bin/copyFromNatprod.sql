show user;
select * from global_name;
set timing on;
set serveroutput on;
whenever sqlerror exit failure rollback;
whenever oserror exit failure rollback;
select 'copy from natprod start time: ' || systimestamp from dual;

prompt sample_parameter
truncate table sample_parameter;
insert /*+ append parallel(4) */ into sample_parameter
select sample_id,
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
  from (select * from (select sample_id, parameter_cd, result_unrnd_va
                         from qw_result
                        where result_web_cd = 'Y' and
                              parameter_cd in ('71999', '50280', '72015', '82047', '72016', '82048', '00003', '00098', '78890', '78891', '82398', '84164'))
                       pivot (max(result_unrnd_va)
                              for parameter_cd in ('71999' V71999, '50280' V50280, '72015' V72015, '82047' V82047, '72016' V72016, '82048' V82048,
                                                   '00003' V00003, '00098' V00098, '78890' V78890, '78891' V78891, '82398' V82398, '84164' V84164))
       ) p2
       left join (select fxd_nm v71999_fxd_nm, fxd_va from fxd where parm_cd = '71999') fxd_71999
         on p2.v71999 = fxd_71999.fxd_va
       left join (select fxd_tx v82398_fxd_tx, fxd_va from fxd where parm_cd = '82398') fxd_82398
         on p2.v82398 = fxd_82398.fxd_va
       left join (select fxd_tx v84164_fxd_tx, fxd_va from fxd where parm_cd = '84164') fxd_84164
         on p2.v84164 = fxd_84164.fxd_va;
commit;

select 'copy from natprod end time: ' || systimestamp from dual;
