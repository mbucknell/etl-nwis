package gov.acwi.wqp.etl.orgData;

import org.springframework.batch.item.ItemProcessor;

import gov.acwi.wqp.etl.Application;
import gov.acwi.wqp.etl.nwis.NwisDistrictCdsByHost;

public class OrgDataProcessor implements ItemProcessor<NwisDistrictCdsByHost, OrgData> {
	
	private static int NWIS_ORG_BASE_ID = 2000000;
	
	@Override
	public OrgData process(NwisDistrictCdsByHost nwisDistrict) throws Exception {
		OrgData orgData = new OrgData();
		orgData.setDataSourceId(Application.DATA_SOURCE_ID);
		orgData.setDataSource(Application.DATA_SOURCE);
		orgData.setOrganizationId(NWIS_ORG_BASE_ID + Integer.parseInt(nwisDistrict.getDistrictCd()));
		orgData.setOrganization("USGS-" + nwisDistrict.getStatePostalCd());
		orgData.setOrganizationName("USGS " + nwisDistrict.getStateName() + " Water Science Center");
		
		return orgData;
	}

}
