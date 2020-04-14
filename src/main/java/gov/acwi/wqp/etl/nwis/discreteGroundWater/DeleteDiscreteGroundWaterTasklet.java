package gov.acwi.wqp.etl.nwis.discreteGroundWater;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import gov.acwi.wqp.etl.TruncateTable;

@Component
@StepScope
public class DeleteDiscreteGroundWaterTasklet extends TruncateTable {

	@Autowired
	public DeleteDiscreteGroundWaterTasklet(@Qualifier("jdbcTemplateNwis")JdbcTemplate jdbcTemplate) {

		super(jdbcTemplate, "discrete_ground_water");
	}

}