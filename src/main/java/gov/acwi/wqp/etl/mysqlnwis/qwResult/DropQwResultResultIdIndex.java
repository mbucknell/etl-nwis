package gov.acwi.wqp.etl.mysqlnwis.qwResult;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
@StepScope
public class DropQwResultResultIdIndex implements Tasklet {

    private final JdbcTemplate jdbcTemplate;

    public DropQwResultResultIdIndex(@Qualifier("jdbcTemplateNwis") JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        jdbcTemplate.execute("drop index if exists qw_result_result_id");
        return RepeatStatus.FINISHED;
    }
}
