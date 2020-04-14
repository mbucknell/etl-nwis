package gov.acwi.wqp.etl.mysqlnwis.gwlevels;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import gov.acwi.wqp.etl.TruncateTable;

@Component
@StepScope
public class DeleteGwLevels extends TruncateTable {

	@Autowired
	public DeleteGwLevels(@Qualifier("jdbcTemplateNwis")JdbcTemplate jdbcTemplate) {
		super(jdbcTemplate, "gw_levels");
	}
}
