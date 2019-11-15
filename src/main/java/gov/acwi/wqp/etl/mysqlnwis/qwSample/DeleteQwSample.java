package gov.acwi.wqp.etl.mysqlnwis.qwSample;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import gov.acwi.wqp.etl.TruncateTable;

@Component
@StepScope
public class DeleteQwSample extends TruncateTable {

	@Autowired
	public DeleteQwSample(@Qualifier("jdbcTemplateNwis")JdbcTemplate jdbcTemplate) {
		super(jdbcTemplate, "qw_sample");
	}

}
