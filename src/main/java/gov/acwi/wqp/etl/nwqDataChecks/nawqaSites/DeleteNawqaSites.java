package gov.acwi.wqp.etl.nwqDataChecks.nawqaSites;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import gov.acwi.wqp.etl.BaseDeleteNwisTable;

@Component
@StepScope
public class DeleteNawqaSites extends BaseDeleteNwisTable {

    @Autowired
    public DeleteNawqaSites(@Qualifier("jdbcTemplateNwis") JdbcTemplate jdbcTemplate) {

        super(jdbcTemplate, "nawqa_sites");
    }
}
