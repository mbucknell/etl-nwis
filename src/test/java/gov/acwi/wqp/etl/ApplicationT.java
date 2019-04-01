package gov.acwi.wqp.etl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Profile;

@SpringBootApplication
@Profile("it")
public class ApplicationT {
	private static final Logger LOG = LoggerFactory.getLogger(ApplicationT.class);

	public static void main(String[] args) {
		LOG.info(args.toString());
		SpringApplication.run(Application.class, args);
	}
}
