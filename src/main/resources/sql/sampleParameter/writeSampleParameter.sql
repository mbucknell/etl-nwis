insert into sample_parameter
select distinct qw_result.sample_id,
    v71999.v v71999,
    v50280.v v50280,
    v72015.v v72015,
    v82047.v v82047,
    v72016.v v72016,
    v82048.v v82048,
    v00003.v v00003,
    v00098.v v00098,
    v78890.v v78890,
    v78891.v v78891,
    v82398.v v82398,
    v84164.v v84164,
    v71999_fxd_nm,
    v82398_fxd_tx,
    v84164_fxd_tx
from qw_result
left outer join (select sample_id, max(result_unrnd_va) v from qw_result where result_web_cd and parameter_cd = '71999'
    group by sample_id) v71999
    on qw_result.sample_id = v71999.sample_id
left outer join (select sample_id, max(result_unrnd_va) v from qw_result where result_web_cd and parameter_cd = '50280'
    group by sample_id) v50280
    on  qw_result.sample_id = v50280.sample_id
left outer join (select sample_id, max(result_unrnd_va) v from qw_result where result_web_cd and parameter_cd = '72015'
    group by sample_id) v72015
    on  qw_result.sample_id = v72015.sample_id
left outer join (select sample_id, max(result_unrnd_va) v from qw_result where result_web_cd and parameter_cd = '82047'
    group by sample_id) v82047
    on  qw_result.sample_id = v82047.sample_id
left outer join (select sample_id, max(result_unrnd_va) v from qw_result where result_web_cd and parameter_cd = '72016'
    group by sample_id) v72016
    on  qw_result.sample_id = v72016.sample_id
left outer join (select sample_id, max(result_unrnd_va) v from qw_result where result_web_cd and parameter_cd = '82048'
    group by sample_id) v82048
    on  qw_result.sample_id = v82048.sample_id
left outer join (select sample_id, max(result_unrnd_va) v from qw_result where result_web_cd and parameter_cd = '00003'
    group by sample_id) v00003
    on  qw_result.sample_id = v00003.sample_id
left outer join (select sample_id, max(result_unrnd_va) v from qw_result where result_web_cd and parameter_cd = '00098'
    group by sample_id) v00098
    on  qw_result.sample_id = v00098.sample_id
left outer join (select sample_id, max(result_unrnd_va) v from qw_result where result_web_cd and parameter_cd = '78890'
    group by sample_id) v78890
    on  qw_result.sample_id = v78890.sample_id
left outer join (select sample_id, max(result_unrnd_va) v from qw_result where result_web_cd and parameter_cd = '78891'
    group by sample_id) v78891
    on  qw_result.sample_id = v78891.sample_id
left outer join (select sample_id, max(result_unrnd_va) v from qw_result where result_web_cd and parameter_cd = '82398'
    group by sample_id) v82398
    on  qw_result.sample_id = v82398.sample_id
left outer join (select sample_id, max(result_unrnd_va) v from qw_result where result_web_cd and parameter_cd = '84164'
    group by sample_id) v84164
    on  qw_result.sample_id = v84164.sample_id
left join (select fxd_nm v71999_fxd_nm, fxd_va from fxd where parm_cd = '71999') fxd_71999
         on cast(v71999.v as numeric) = fxd_71999.fxd_va
left join (select fxd_tx v82398_fxd_tx, fxd_va from fxd where parm_cd = '82398') fxd_82398
     on cast(v82398.v as numeric) = fxd_82398.fxd_va
left join (select fxd_tx v84164_fxd_tx, fxd_va from fxd where parm_cd = '84164') fxd_84164
     on cast(v84164.v as numeric) = fxd_84164.fxd_va;