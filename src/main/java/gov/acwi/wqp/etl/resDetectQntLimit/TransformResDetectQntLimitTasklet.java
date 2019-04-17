package gov.acwi.wqp.etl.resDetectQntLimit;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.FileCopyUtils;

@Component
@StepScope
public class TransformResDetectQntLimitTasklet implements Tasklet {

	private final JdbcTemplate jdbcTemplate;

	@Autowired
	public TransformResDetectQntLimitTasklet(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	@Value("classpath:sql/resDetectQntLimit/writeResDetectQntLimit.sql")
	private Resource executeResource;

	@Override
	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		jdbcTemplate.execute(new String(FileCopyUtils.copyToByteArray(executeResource.getInputStream())));
		return RepeatStatus.FINISHED;
	}
}
