package gov.acwi.wqp.etl.mysqlnwis.stat;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import gov.acwi.wqp.etl.TruncateTable;

@Component
@StepScope
public class DeleteStat extends TruncateTable {

	@Autowired
	public DeleteStat(@Qualifier("jdbcTemplateNwis")JdbcTemplate jdbcTemplate) {
		super(jdbcTemplate, "stat");
	}

}
