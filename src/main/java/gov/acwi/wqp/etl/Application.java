package gov.acwi.wqp.etl;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Application{


	public static final Integer DATA_SOURCE_ID = 2;
	public static final String DATA_SOURCE = "NWIS";
	

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}
}
