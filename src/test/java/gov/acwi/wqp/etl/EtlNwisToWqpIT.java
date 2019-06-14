package gov.acwi.wqp.etl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.ExpectedDatabase;
import com.github.springtestdbunit.assertion.DatabaseAssertionMode;

public class EtlNwisToWqpIT extends NwisBaseFlowIT {

    @Autowired
    @Qualifier("nwisToWqpFlow")
    private Flow nwisToWqpFlow;

    @Test
    // Lookups and mysql data tables
    @DatabaseSetup(value = "classpath:/testResult/wqp/monitoringLocation/empty.xml")
    @DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/altitudeMethod/altitudeMethod.xml")
    @DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/aqfr/aqfr.xml")
    @DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/aquiferType/aquiferType.xml")
    @DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/bodyPart/bodyPart.xml")
    @DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/fxd/fxd.xml")
    @DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/hydCondCd/hydCondCd.xml")
    @DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/hydEventCd/hydEventCd.xml")
    @DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/latLongAccuracy/latLongAccuracy.xml")
    @DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/latLongMethod/latLongMethod.xml")
    @DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/methWithCit/methWithCit.xml")
    @DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/natAqfr/natAqfr.xml")
    @DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/nwisDistrictCdsByHost/nwisDistrictCdsByHost.xml")
    @DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/nwisWqxMediumCd/nwisWqxMediumCd.xml")
    @DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/nwisWqxRptLevCd/nwisWqxRptLevCd.xml")    @DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/parm/parm.xml")
    @DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/parmMeth/parmMeth.xml")
    @DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/qwResult/qwResult.xml")
    @DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/qwSample/qwSample.xml")
    @DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/sitefile/sitefile.xml")
    @DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/siteTp/siteTp.xml")

    // NWIS base data
    @ExpectedDatabase(connection = CONNECTION_NWIS, value = "classpath:/testResult/endToEnd/sampleParameter.xml", assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED)
    @ExpectedDatabase(value = "classpath:/testResult/endToEnd/orgData.xml", assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED)
    @ExpectedDatabase(value = "classpath:/testResult/endToEnd/projectData.xml", assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED)
    @ExpectedDatabase(value = "classpath:/testResult/endToEnd/monitoringLocation.xml", assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED)
    @ExpectedDatabase(value = "classpath:/testResult/endToEnd/biologicalHabitatMetric.xml", assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED)
    @ExpectedDatabase(value = "classpath:/testResult/endToEnd/activity.xml", assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED)
    @ExpectedDatabase(value = "classpath:/testResult/endToEnd/activityMetric.xml", assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED)
    @ExpectedDatabase(value = "classpath:/testResult/endToEnd/result.xml", assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED)
    @ExpectedDatabase(value = "classpath:/testResult/endToEnd/resDetectQntLimit.xml", assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED)
    @ExpectedDatabase(value = "classpath:/testResult/endToEnd/projectMLWeighting.xml", assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED)
    public void nwisToWqpFlowTest() {
        Job nwisToWqpEtlFlowTest = jobBuilderFactory.get("nwisToWqpEtlFlowTest")
                .start(nwisToWqpFlow)
                .build()
                .build();
        jobLauncherTestUtils.setJob(nwisToWqpEtlFlowTest);
        try {
            JobExecution jobExecution = jobLauncherTestUtils.launchJob(testJobParameters);
            assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getLocalizedMessage());
        }
    }

}
