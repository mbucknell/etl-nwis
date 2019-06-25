package gov.acwi.wqp.etl.publicSrsnames;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcCall;
import org.springframework.stereotype.Component;

import gov.acwi.wqp.etl.EtlConstantUtils;

@Component
@StepScope
public class TransformPublicSrsnamesTask implements Tasklet {

	private final JdbcTemplate jdbcTemplate;
	private final String wqpSchemaName;

	public TransformPublicSrsnamesTask (
			JdbcTemplate jdbcTemplate,
			@Value(EtlConstantUtils.VALUE_JOB_PARM_WQP_SCHEMA) String wqpSchemaName) {
		this.jdbcTemplate = jdbcTemplate;
		this.wqpSchemaName = wqpSchemaName;
	}

	@Override
	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		SimpleJdbcCall call = new SimpleJdbcCall(jdbcTemplate)
				.withSchemaName(wqpSchemaName)
				.withFunctionName("transform_public_srsnames");
		call.execute();
		return RepeatStatus.FINISHED;
	}
}
