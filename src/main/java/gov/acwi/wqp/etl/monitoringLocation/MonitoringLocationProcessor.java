package gov.acwi.wqp.etl.monitoringLocation;


import java.math.BigDecimal;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import gov.acwi.wqp.etl.BaseProcessor;
import gov.acwi.wqp.etl.ConfigurationService;
import gov.acwi.wqp.etl.nwis.monitoringLocation.NwisMonitoringLocation;

@Component
public class MonitoringLocationProcessor extends BaseProcessor implements ItemProcessor<NwisMonitoringLocation, MonitoringLocation> {

	private final ConfigurationService configurationService;

	private static final String DEFAULT_GEOPOSITIONING_METHOD = "Unknown";
	private  static final String DEFAULT_HDATUM_ID_CODE = "Unknown";
	private  static final String DEFAULT_ELEVATION_UNIT = "feet";
	private  static final String DEFAULT_DRAIN_AREA_UNIT = "sq mi";
	private  static final String DEFAULT_VERTICAL_ACCURACY_UNIT = "feet";
	private  static final String DEFAULT_DEPTH_UNIT = "ft";

	@Autowired
	public MonitoringLocationProcessor(ConfigurationService configurationService) {
		this.configurationService = configurationService;
	}
	
	@Override
	public MonitoringLocation process(NwisMonitoringLocation nwisML) throws Exception {
		MonitoringLocation monitoringLocation = new MonitoringLocation();
		
		String nwisMLAltDatumCd = nwisML.getAltDatumCd();
		String nwisMLAltVa = nwisML.getAltVa();
		String nwisMLWellDepthVa = nwisML.getWellDepthVa();
		String nwisMLHoleDepthVa = nwisML.getHoleDepthVa();
		
		monitoringLocation.setDataSourceId(configurationService.getEtlDataSourceId());
		monitoringLocation.setDataSource(configurationService.getEtlDataSource());
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
		monitoringLocation.setMapScale(StringUtils.trimToNull(nwisML.getMapScaleFc()));
		monitoringLocation.setGeopositioningMethod(
				nwisML.getLatLongMethodDescription() == null ? DEFAULT_GEOPOSITIONING_METHOD : nwisML.getLatLongMethodDescription());
		monitoringLocation.setHdatumIdCode(
				nwisML.getDecCoordDatumCd() == null ? DEFAULT_HDATUM_ID_CODE : nwisML.getDecCoordDatumCd());
		monitoringLocation.setElevationValue(getElevationValue(nwisMLAltDatumCd, nwisMLAltVa));
		monitoringLocation.setElevationUnit(
				nwisMLAltDatumCd == null && nwisMLAltVa == null ? null : DEFAULT_ELEVATION_UNIT);
		monitoringLocation.setElevationMethod(nwisML.getAltitudeMethodDescription());
		monitoringLocation.setVdatumIdCode(nwisML.getAltVa() == null ? null : nwisML.getAltDatumCd());
		monitoringLocation.setDrainAreaValue(getBigDecimal(nwisML.getDrainAreaVa()));
		monitoringLocation.setDrainAreaUnit(nwisML.getDrainAreaVa() == null ? null : DEFAULT_DRAIN_AREA_UNIT);
		monitoringLocation.setContribDrainAreaValue(
				isNonNullAndEqual(nwisML.getContribDrainAreaVa(), ".") ? BigDecimal.ZERO : getBigDecimal(nwisML.getContribDrainAreaVa()));
		monitoringLocation.setContribDrainAreaUnit(nwisML.getContribDrainAreaVa() == null ? null : DEFAULT_DRAIN_AREA_UNIT);
		monitoringLocation.setGeopositionAccyValue(nwisML.getLatLongAccuracy());
		monitoringLocation.setGeopositionAccyUnit(nwisML.getLatLongAccuracyUnit());
		monitoringLocation.setVerticalAccuracyValue(
				nwisMLAltDatumCd != null && nwisMLAltVa != null ? StringUtils.trimToNull(nwisML.getAltAcyVa()) : null);
		monitoringLocation.setVerticalAccuracyUnit(
				nwisMLAltDatumCd != null && nwisMLAltVa != null && nwisML.getAltAcyVa() != null ? DEFAULT_VERTICAL_ACCURACY_UNIT : null);
		monitoringLocation.setNatAqfrName(nwisML.getNatAqfrName());
		monitoringLocation.setAqfrName(nwisML.getAqfrNm());
		monitoringLocation.setAqfrTypeName(nwisML.getAquiferTypeDescription());
		monitoringLocation.setConstructionDate(nwisML.getConstructionDt());
		monitoringLocation.setWellDepthValue(
				isNonNullAndEqual(nwisMLWellDepthVa, ".") && isNonNullAndEqual(nwisMLWellDepthVa, "-") ? BigDecimal.ZERO : getBigDecimal(nwisMLWellDepthVa));
		monitoringLocation.setWellDepthUnit(nwisMLWellDepthVa == null ? null : DEFAULT_DEPTH_UNIT);
		monitoringLocation.setHoleDepthValue(
				isNonNullAndEqual(nwisMLHoleDepthVa, ".") && isNonNullAndEqual(nwisMLHoleDepthVa, "-") ? BigDecimal.ZERO : getBigDecimal(nwisMLHoleDepthVa));
		monitoringLocation.setHoleDepthUnit(nwisMLHoleDepthVa == null ? null : DEFAULT_DEPTH_UNIT);
		
		return monitoringLocation;		
	}
	
	private String getElevationValue(String altDatumCd, String altVa) {
		if (altDatumCd == null) {
			return null;
		} else if (isNonNullAndEqual(altVa, ".")) {
			return "0";	
		} else {
			return StringUtils.trimToNull(altVa);
		}
	}
	
	private BigDecimal getBigDecimal(String string) {
		if (NumberUtils.isCreatable(string)) {
			return NumberUtils.createBigDecimal(string);
		} else {
			return null;
		}
	}

}
