show user;
select * from global_name;
set timing on;
set serveroutput on;
whenever sqlerror exit failure rollback;
whenever oserror exit failure rollback;
select 'validate dw start time: ' || systimestamp from dual;

begin
  declare old_rows    int;
          new_rows    int;
          pass_fail   varchar2(15);
          end_job     boolean := false;

begin
	
	dbms_output.put_line('validating...');

	dbms_output.put_line('... pc_result');
	select count(*) into old_rows from pc_result partition (pc_result_nwis);
	select count(*) into new_rows from pc_result_swap_nwis;
	if new_rows > 70000000 and new_rows > old_rows - 9000000 then
    	pass_fail := 'PASS';
    	end_job := true;
    else
        pass_fail := 'FAIL';
        $IF $$empty_db $THEN
    		pass_fail := 'PASS empty_db';
        $END
    end if;
    dbms_output.put_line(pass_fail || ': table comparison for pc_result: was ' || trim(to_char(old_rows, '999,999,999')) || ', now ' || trim(to_char(new_rows, '999,999,999')));

    dbms_output.put_line('... station');
    select count(*) into old_rows from station partition (station_nwis);
    select count(*) into new_rows from station_swap_nwis;
    if new_rows > 1400000 and new_rows > old_rows - 100000 then
        pass_fail := 'PASS';
    	end_job := true;
    else
        pass_fail := 'FAIL';
        $IF $$empty_db $THEN
              pass_fail := 'PASS empty_db';
        $END
    end if;
    dbms_output.put_line(pass_fail || ': table comparison for station: was ' || trim(to_char(old_rows, '999,999,999')) || ', now ' || trim(to_char(new_rows, '999,999,999')));

  	if end_job then
    	raise_application_error(-20666, 'Failed to pass one or more validation checks.');
  	end if;

end;
end;
/

select 'validate dw tables end time: ' || systimestamp from dual;
