package gov.acwi.wqp.etl;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Application{

	public static final String DATASOURCE_MYSQLNWIS_QUALIFER = "dataSourceMysqlnwis";
	public static final String DATASOURCE_NWIS_QUALIFIER = "dataSourceNwis";
	public static final String DATASOURCE_NWQ_DATA_CHECKS_QUALIFIER = "dataSourceNwqDataChecks";

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}
}
