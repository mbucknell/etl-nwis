package gov.acwi.wqp.etl;


import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Profile;

@SpringBootApplication
@EnableBatchProcessing
@Profile("default")
public class Application implements CommandLineRunner {

	private static final Logger LOG = LoggerFactory.getLogger(Application.class);

	public static final String JOB_ID = "jobId";
	public static final Integer DATA_SOURCE_ID = 2;
	public static final String DATA_SOURCE = "NWIS";
	
	@Autowired
	private Job nwisEtl;
	
	@Autowired
	private JobLauncher jobLauncher;
	@Autowired
	private JobExplorer jobExplorer;
	
	public static void main(String[] args) {
		LOG.info(args.toString());
		SpringApplication.run(Application.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
//		JobParameters parameters = new JobParametersBuilder(jobExplorer)
//				.addDate(JOB_ID, new Date(), true)
//				.addString("wqpDataSource", DATA_SOURCE)
//				.addString("schemaName", "wqp") // TODO: Should come from command line?
//				.toJobParameters();
//		jobLauncher.run(nwisEtl, parameters);

	}
}
