package gov.acwi.wqp.etl.nwis.sampleParameter;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import gov.acwi.wqp.etl.nwis.TruncateTable;

@Component
@StepScope
public class DeleteSampleParameterTasklet extends TruncateTable {

    @Autowired
    public DeleteSampleParameterTasklet(@Qualifier("jdbcTemplateNwis")JdbcTemplate jdbcTemplate) {

        super(jdbcTemplate, "sample_parameter");
    }

}