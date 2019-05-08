package gov.acwi.wqp.etl;

import java.time.LocalDateTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("!test & !it")
public class JobCommandLineRunner implements CommandLineRunner {

	private static final Logger LOG = LoggerFactory.getLogger(JobCommandLineRunner.class);

	@Autowired
	private Job job;
	@Autowired
	private JobLauncher jobLauncher;
	@Autowired
	private JobExplorer jobExplorer;
	@Autowired
	private ConfigurationService configurationService;

	@Override
	public void run(String... args) throws Exception {
		JobParameters parameters = new JobParametersBuilder(jobExplorer)
				.addString(EtlConstantUtils.JOB_ID, "44163", true)//LocalDateTime.now().toString(), true)
				.addString(EtlConstantUtils.JOB_PARM_DATA_SOURCE_ID, configurationService.getEtlDataSourceId().toString(), true)
				.addString(EtlConstantUtils.JOB_PARM_DATA_SOURCE, configurationService.getEtlDataSource().toLowerCase(), true)
				.addString(EtlConstantUtils.JOB_PARM_WQP_SCHEMA, configurationService.getWqpSchemaName(), false)
				.addString(EtlConstantUtils.JOB_PARM_GEO_SCHEMA, configurationService.getGeoSchemaName(), false)
				.toJobParameters();
		try {
			JobExecution jobExecution = jobLauncher.run(job, parameters);
			if (null == jobExecution 
					|| ExitStatus.UNKNOWN.getExitCode().contentEquals(jobExecution.getExitStatus().getExitCode())
					|| ExitStatus.FAILED.getExitCode().contentEquals(jobExecution.getExitStatus().getExitCode())
					|| ExitStatus.STOPPED.getExitCode().contentEquals(jobExecution.getExitStatus().getExitCode())) {
				throw new JobExecutionException("Job did not complete as planned.");
			}
			System.exit(0);
		} catch (JobInstanceAlreadyCompleteException e) {
			LOG.info(e.getLocalizedMessage());
			System.exit(0);
		}
	}

}
