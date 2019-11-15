package gov.acwi.wqp.etl.monitoringLocation;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.FileCopyUtils;

@Configuration
public class TransformMonitoringLocation implements Tasklet {

	private final JdbcTemplate jdbcTemplate;

	@Value("classpath:sql/monitoringLocation/monitoringLocation.sql")
	private Resource resource;

	@Autowired
	public TransformMonitoringLocation(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	@Override
	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		String sql = new String(FileCopyUtils.copyToByteArray(resource.getInputStream()));
		jdbcTemplate.execute(sql);
		return RepeatStatus.FINISHED;
	}

}
