package gov.acwi.wqp.etl.mysqlnwis.sitefile;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import gov.acwi.wqp.etl.BaseDeleteNwisTable;

@Component
@StepScope
public class DeleteSitefile extends BaseDeleteNwisTable {
	
	@Autowired
	public DeleteSitefile(@Qualifier("jdbcTemplateNwis")JdbcTemplate jdbcTemplate) {
		super(jdbcTemplate, "sitefile");
	}

}
