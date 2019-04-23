package gov.acwi.wqp.etl.orgData;

import gov.acwi.wqp.etl.ConfigurationService;
import gov.acwi.wqp.etl.nwis.NwisDistrictCdsByHost;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class OrgDataProcessor implements ItemProcessor<NwisDistrictCdsByHost, OrgData> {

	private final ConfigurationService configurationService;
	
	private static int NWIS_ORG_BASE_ID = 2000000;

	@Autowired
	public OrgDataProcessor(ConfigurationService configurationService) {
		this.configurationService = configurationService;
	}
	
	@Override
	public OrgData process(NwisDistrictCdsByHost nwisDistrict) throws Exception {
		OrgData orgData = new OrgData();
		orgData.setDataSourceId(configurationService.getEtlDataSourceId());
		orgData.setDataSource(configurationService.getEtlDataSource());
		orgData.setOrganizationId(NWIS_ORG_BASE_ID + Integer.parseInt(nwisDistrict.getDistrictCd()));
		orgData.setOrganization("USGS-" + nwisDistrict.getStatePostalCd());
		orgData.setOrganizationName("USGS " + nwisDistrict.getStateName() + " Water Science Center");
		
		return orgData;
	}

}
