create or replace function transform_public_srsnames ()
returns void
language plpgsql
as $$
declare
begin
    execute 'truncate table public_srsnames';

    execute
        '
        with wqp_crosswalk as (
            select
                parm_alias.parm_cd,
                parm_alias.parm_alias_nm nm,
                parm_alias.parm_alias_rev_dt rev_dt
            from nwis.parm_alias
            where parm_alias_cd = ''WQPCROSSWALK''
            ),
         srsname_crosswalk as (
            select
                parm_alias.parm_cd,
                parm_alias.parm_alias_nm nm,
                parm_alias.parm_alias_rev_dt rev_dt
            from nwis.parm_alias
            where parm_alias_cd = ''SRSNAME''
            )
        insert into public_srsnames (
            data_source_id,
            parm_cd,
            description,
            characteristicname,
            measureunitcode,
            resultsamplefraction,
            resulttemperaturebasis,
            resultstatisticalbasis,
            resulttimebasis,
            resultweightbasis,
            resultparticlesizebasis,
            last_rev_dt,
            max_last_rev_dt)
        select
            2 data_source_id,
            parm.parm_cd,
            parm.parm_ds description,
            parm_alias.parm_alias_nm characteristicname,
            parm.parm_unt_tx measureunitcode,
            parm.parm_frac_tx resultsamplefraction,
            parm.parm_temp_tx resulttemperaturebasis,
            parm.parm_stat_tx resultstatisticalbasis,
            parm.parm_tm_tx resulttimebasis,
            parm.parm_wt_tx resultweightbasis,
            parm.parm_size_tx resultparticlesizebasis,
            case
                when parm.parm_rev_dt > parm_alias.parm_alias_rev_dt
                   then parm.parm_rev_dt
                 else parm_alias.parm_alias_rev_dt
               end last_rev_dt,
            max(case
                    when parm.parm_rev_dt > parm_alias.parm_alias_rev_dt
                       then parm.parm_rev_dt
                    else parm_alias.parm_alias_rev_dt
                end) over () max_last_rev_dt
        from nwis.parm
        join (
            select
                coalesce(wqp_crosswalk.parm_cd, srsname_crosswalk.parm_cd) parm_cd,
                coalesce(wqp_crosswalk.nm, srsname_crosswalk.nm) parm_alias_nm,
                coalesce(wqp_crosswalk.rev_dt, srsname_crosswalk.rev_dt) parm_alias_rev_dt
            from wqp_crosswalk
            full join srsname_crosswalk on wqp_crosswalk.parm_cd = srsname_crosswalk.parm_cd
            ) parm_alias
        on parm.parm_cd = parm_alias.parm_cd
        where parm_public_fg = ''Y''
        order by parm_cd';
end
$$