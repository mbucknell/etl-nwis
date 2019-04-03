package gov.acwi.wqp.etl.monitoringLocation;


import org.apache.commons.lang3.StringUtils;
import org.springframework.batch.item.ItemProcessor;

import gov.acwi.wqp.etl.Application;
import gov.acwi.wqp.etl.nwis.NwisMonitoringLocation;

public class MonitoringLocationProcessor implements ItemProcessor<NwisMonitoringLocation, MonitoringLocation> {
	
	public static final String DEFAULT_GEOPOSITIONING_METHOD = "Unknown";
	public static final String DEFAULT_HDATUM_ID_CODE = "Unknown";
	public static final String DEFAULT_ELEVATION_UNIT = "feet";
	
	@Override
	public MonitoringLocation process(NwisMonitoringLocation nwisML) throws Exception {
		MonitoringLocation monitoringLocation = new MonitoringLocation();
		
		String nwisMLAltDatumCd = nwisML.getAltDatumCd();
		String nwisMLAltVa = nwisML.getAltVa();
		
		monitoringLocation.setDataSourceId(Application.DATA_SOURCE_ID);
		monitoringLocation.setDataSource(Application.DATA_SOURCE);
		monitoringLocation.setStationId(nwisML.getSiteId());
		monitoringLocation.setSiteId(nwisML.getAgencyCd() + "-" + nwisML.getSiteNo());
		monitoringLocation.setOrganization(nwisML.getOrganizationId());
		monitoringLocation.setSiteType(nwisML.getPrimarySiteType());
		monitoringLocation.setHuc(nwisML.getCalculatedHuc12() == null ? nwisML.getHucCd() : nwisML.getCalculatedHuc12());
		monitoringLocation.setGovernmentalUnitCode(
				nwisML.getCountryCd() + ":" + nwisML.getStateCd() + ":" + nwisML.getCountyCd());
		monitoringLocation.setGeom(nwisML.getGeom());
		monitoringLocation.setStationName(StringUtils.trimToNull(nwisML.getStationNm()));
		monitoringLocation.setOrganizationName(nwisML.getOrganizationName());
		monitoringLocation.setStationTypeName(nwisML.getStationTypeName());
		monitoringLocation.setLatitude(nwisML.getDecLatVa());
		monitoringLocation.setLongitude(nwisML.getDecLongVa());
		monitoringLocation.setMapScale(nwisML.getMapScaleFc());
		monitoringLocation.setGeopositioningMethod(
				nwisML.getLatLongMethodDescription() == null ? DEFAULT_GEOPOSITIONING_METHOD : nwisML.getLatLongMethodDescription());
		monitoringLocation.setHdatumIdCode(
				nwisML.getDecCoordDatumCd() == null ? DEFAULT_HDATUM_ID_CODE : nwisML.getDecCoordDatumCd());
		monitoringLocation.setElevationValue(getElevationValue(nwisMLAltDatumCd, nwisMLAltVa));
		monitoringLocation.setElevationUnit(
				nwisMLAltDatumCd == null && nwisMLAltVa == null ? null : DEFAULT_ELEVATION_UNIT);
		
		return monitoringLocation;		
	}
	
	private String getElevationValue(String altDatumCd, String altVa) {
		if (altDatumCd == null) {
			return null;
		} else if (altVa == ".") {
			return "0";	
		} else {
			return StringUtils.trimToNull(altVa);
		}
	}

}
