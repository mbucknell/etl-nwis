package gov.acwi.wqp.etl;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {
	
	@Autowired
	private JobBuilderFactory jobBuilderFactory;
	
	@Autowired
	@Qualifier("mySqlNwisExtractFlow")
	private Flow mySqlNwisExtractFlow;

	@Autowired
	@Qualifier("sampleParameterFlow")
	private Flow sampleParameterFlow;
	
	@Autowired
	@Qualifier("orgDataFlow")
	private Flow orgDataFlow;

	@Autowired
	@Qualifier("projectDataFlow")
	private Flow projectDataFlow;
	
	@Autowired
	@Qualifier("monitoringLocationFlow")
	private Flow monitoringLocationFlow;

	@Autowired
	@Qualifier("biologicalHabitatMetricFlow")
	private Flow biologicalHabitatMetricFlow;
	
	@Autowired
	@Qualifier("activityFlow")
	private Flow activityFlow;

	@Autowired
	@Qualifier("activityMetricFlow")
	private Flow activityMetricFlow;
	
	@Autowired
	@Qualifier("resultFlow")
	private Flow resultFlow;
	
	@Autowired
	@Qualifier("resDetectQntLimitFlow")
	private Flow resDetectQntLimitFlow;

	@Autowired
	@Qualifier("projectMLWeightingFlow")
	private Flow projectMLWeightingFlow;
	
	@Autowired
	@Qualifier("createSummariesFlow")
	private Flow createSummariesFlow;

	@Autowired
	@Qualifier("createLookupCodesFlow")
	private Flow createLookupCodesFlow;

	@Autowired
	@Qualifier("databaseFinalizeFlow")
	private Flow databaseFinalizeFlow;


	@Bean
	public Job nwisEtl() {
		return jobBuilderFactory.get("WQP_NWIS_ETL")
				//.start(mySqlNwisExtractFlow)
				.start(sampleParameterFlow)
				.next(orgDataFlow)
				.next(projectDataFlow)
				.next(monitoringLocationFlow)
				.next(biologicalHabitatMetricFlow)
				.next(activityFlow)
				.next(activityMetricFlow)
				//.next(resultFlow)
				//.next(resDetectQntLimitFlow)
				//.next(projectMLWeightingFlow)
				//.next(createSummariesFlow)
				//.next(createLookupCodesFlow)
				//.next(databaseFinalizeFlow)
				.build()
				.build();
	}
		
}
