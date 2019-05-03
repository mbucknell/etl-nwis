package gov.acwi.wqp.etl.mysqlnwis;

import java.util.HashMap;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcCall;

public abstract class BaseDeleteTable implements Tasklet {

	
	private final JdbcTemplate jdbcTemplate;
	private String tableName;

	
	private static final String FUNCTION_NAME = "truncate_table";
	private static final String SCHEMA_NAME = "nwis";
	
	public BaseDeleteTable(JdbcTemplate jdbcTemplate, String tableName) {
		this.jdbcTemplate = jdbcTemplate;
		this.tableName = tableName;
	}

	@Override
	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		SimpleJdbcCall call = new SimpleJdbcCall(jdbcTemplate).withSchemaName(SCHEMA_NAME).withFunctionName(FUNCTION_NAME);
		
		HashMap<String, Object> params = new HashMap<>();
		params.put("table_name", tableName);

		call.execute(params);
		
		return RepeatStatus.FINISHED;
	}

}
