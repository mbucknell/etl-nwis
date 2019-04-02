show user;
select * from global_name;
set timing on;
set serveroutput on;
whenever sqlerror exit failure rollback;
whenever oserror exit failure rollback;
select 'create public_srsnames start time: ' || systimestamp from dual;

begin
    declare type cursor_type is ref cursor;
            suffix      varchar2(6 char);
            query       varchar2(4000);
            c           cursor_type;
            old_rows    int;
            new_rows    int;
            pass_fail   varchar2(15);
            situation   varchar2(200);

    begin

        select '_' || to_char(nvl(max(to_number(substr(table_name, length('PUBLIC_SRSNAMES_') + 1)) + 1), 1), 'fm00000')
          into suffix
          from user_tables
         where translate(table_name, '0123456789', '0000000000') = 'PUBLIC_SRSNAMES_00000';
         
        dbms_output.put_line('using ' || suffix || ' for the suffix.');
        
        dbms_output.put_line('creating public_srsnames...');

        execute immediate 'create table public_srsnames' || suffix || q'! compress pctfree 0 nologging as
        select parm.parm_cd,
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
          from nwis_ws_star.parm
               join (select parm_cd, nvl(wqpcrosswalk_nm, srsname_nm) parm_alias_nm, nvl(wqpcrosswalk_dt, srsname_dt) parm_alias_rev_dt
                       from (select parm_cd, parm_alias_cd, parm_alias_nm, parm_alias_rev_dt
                               from nwis_ws_star.parm_alias
                              where parm_alias_cd in ('WQPCROSSWALK', 'SRSNAME'))
                              pivot (max(parm_alias_nm) as nm, max(parm_alias_rev_dt) as dt
                                for parm_alias_cd in ('WQPCROSSWALK' wqpcrosswalk, 'SRSNAME' srsname))
                      where nvl(wqpcrosswalk_nm, srsname_nm) is not null) parm_alias
                 on parm.parm_cd = parm_alias.parm_cd
         where parm_public_fg = 'Y'
           order by 1!';

          execute immediate 'grant select on public_srsnames' || suffix || ' to wqp_user';

          dbms_output.put_line('analyze public_srsnames...');
          dbms_stats.gather_table_stats(user, 'PUBLIC_SRSNAMES' || suffix, null, 10, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);

        select count(*) into old_rows from public_srsnames;
          query := 'select count(*) from public_srsnames' || suffix;
          open c for query;
          fetch c into new_rows;
          close c;
          

        if new_rows > 9000 and new_rows > old_rows - 500 then
            pass_fail := 'PASS';
              query := 'create or replace synonym public_srsnames for public_srsnames' || suffix;
               dbms_output.put_line(query);
              execute immediate query;

            for i in (select table_name
                        from user_tables
                       where table_name != 'PUBLIC_SRSNAMES' || suffix and
                             translate(table_name, '0123456789', '0000000000') = 'PUBLIC_SRSNAMES_00000')
            loop
                  query := 'drop table ' || i.table_name || ' cascade constraints purge';
                  dbms_output.put_line(query);
                  execute immediate query;
            end loop;
        else
            pass_fail := 'FAIL';
        end if;
        
        situation := pass_fail || ': table comparison for public_srsnames: was ' || trim(to_char(old_rows, '999,999,999')) || ', now ' || trim(to_char(new_rows, '999,999,999'));
          dbms_output.put_line(situation);
    end;
end;
/

select 'create public_srsnames end time: ' || systimestamp from dual;
