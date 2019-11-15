package gov.acwi.wqp.etl;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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
	private JobOperator jobOperator;
	@Autowired
	private JobExplorer jobExplorer;
	@Autowired
	private ConfigurationService configurationService;

	// Load date as a system property because it contains spaces
	@Value("${app.job.id.date}")
	private String jobIdDate;

	@Override
	public void run(String... args) throws Exception {
		// probe the args to see if any are being passed into the app
		// Why, you say? Because the args have not been used until now.
		LOG.info("Batch execution with parameter arguments " + Arrays.asList(args));
		// check the system parameter value also
		LOG.info("Batch execution job id date '" + jobIdDate + "'");

		ExitStatus exitStatus = null;
		try {
			if (!jobOperator.getRunningExecutions(job.getName()).isEmpty()) {
				LOG.info("This run cancelled, there is already a job running for " + job.getName());
			}
		} catch (NoSuchJobException e) {
			// OUCH! THis is the happy path controlled by an exception.
			LOG.info("Attempting to restart " + job.getName());
			exitStatus = startJob(jobIdDate);
		}
		if (null == exitStatus
				|| ExitStatus.UNKNOWN.getExitCode().contentEquals(exitStatus.getExitCode())
				|| ExitStatus.FAILED.getExitCode().contentEquals(exitStatus.getExitCode())
				|| ExitStatus.STOPPED.getExitCode().contentEquals(exitStatus.getExitCode())) {
			throw new RuntimeException("Job did not complete as planned.");
		}
	}

	protected JobParameters getJobParametersBuilder(String jobIdDate) {
		return new JobParametersBuilder(jobExplorer)
				.addString(EtlConstantUtils.JOB_ID, jobIdDate, true)
				.addString(EtlConstantUtils.JOB_PARM_DATA_SOURCE_ID, configurationService.getEtlDataSourceId().toString(), true)
				.addString(EtlConstantUtils.JOB_PARM_DATA_SOURCE, configurationService.getEtlDataSource().toLowerCase(), true)
				.addString(EtlConstantUtils.JOB_PARM_WQP_SCHEMA, configurationService.getWqpSchemaName(), false)
				.addString(EtlConstantUtils.JOB_PARM_GEO_SCHEMA, configurationService.getGeoSchemaName(), false)
				.toJobParameters();
	}

	protected ExitStatus startJob(String jobIdDate) throws Exception {
		JobParameters parameters = getJobParametersBuilder(jobIdDate);

		try {
			return jobLauncher.run(job, parameters).getExitStatus();
		} catch (JobInstanceAlreadyCompleteException e) {
			// log the new date job ID
			LOG.info("Job " + job.getName() + " for '" + parameters.getString(EtlConstantUtils.JOB_ID) + "' has already completed successfully.");
			return ExitStatus.COMPLETED; // If it was already done then things are A.O.K.
		}
	}
}

