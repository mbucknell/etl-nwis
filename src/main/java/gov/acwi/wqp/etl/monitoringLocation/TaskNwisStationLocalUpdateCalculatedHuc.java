package gov.acwi.wqp.etl.monitoringLocation;

import javax.sql.DataSource;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.FileCopyUtils;

@Configuration
public class TaskNwisStationLocalUpdateCalculatedHuc implements Tasklet {

    @Autowired
    @Qualifier("dataSourceNwis")
    private DataSource dataSourceNwis;

    @Value("classpath:sql/calculateHuc12.sql")
    private Resource sqlResource;


    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSourceNwis);

        jdbcTemplate.update(new String(FileCopyUtils.copyToByteArray(sqlResource.getInputStream())));

        return RepeatStatus.FINISHED;
    }
}
