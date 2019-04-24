package gov.acwi.wqp.etl.nwis;

import java.math.BigDecimal;

import org.postgis.PGgeometry;

public class NwisMonitoringLocation {
	
	private int siteId;
	private String agencyCd;
	private String siteNo;
	private String organizationId;
	private String primarySiteType;
	private String calculatedHuc12;
	private String hucCd;
	private String countryCd;
	private String stateCd;
	private String countyCd;
	private PGgeometry geom;
	private String stationNm;
	private String organizationName;
	private String stationTypeName;
	private BigDecimal decLatVa;
	private BigDecimal decLongVa;
	private String mapScaleFc;
	private String latLongMethodDescription;
	private String decCoordDatumCd;
	private String altDatumCd;
	private String altVa;
	private String altAcyVa;
	private String altitudeMethodDescription;
	private String drainAreaVa;
	private String contribDrainAreaVa;
	private String latLongAccuracy;
	private String latLongAccuracyUnit;
	private String natAqfrName;
	private String aqfrNm;
	private String aquiferTypeDescription;
	private String constructionDt;
	private String wellDepthVa;
	private String holeDepthVa;
	
	public int getSiteId() {
		return siteId;
	}
	public void setSiteId(int siteId) {
		this.siteId = siteId;
	}
	public String getAgencyCd() {
		return agencyCd;
	}
	public void setAgencyCd(String agencyCd) {
		this.agencyCd = agencyCd;
	}
	public String getSiteNo() {
		return siteNo;
	}
	public void setSiteNo(String siteNo) {
		this.siteNo = siteNo;
	}
	public String getOrganizationId() {
		return organizationId;
	}
	public void setOrganizationId(String organizationId) {
		this.organizationId = organizationId;
	}
	public String getPrimarySiteType() {
		return primarySiteType;
	}
	public void setPrimarySiteType(String primarySiteType) {
		this.primarySiteType = primarySiteType;
	}
	public String getCalculatedHuc12() {
		return calculatedHuc12;
	}
	public void setCalculatedHuc12(String calculatedHuc12) {
		this.calculatedHuc12 = calculatedHuc12;
	}
	public String getHucCd() {
		return hucCd;
	}
	public void setHucCd(String hucCd) {
		this.hucCd = hucCd;
	}
	public String getCountryCd() {
		return countryCd;
	}
	public void setCountryCd(String countryCd) {
		this.countryCd = countryCd;
	}
	public String getStateCd() {
		return stateCd;
	}
	public void setStateCd(String stateCd) {
		this.stateCd = stateCd;
	}
	public String getCountyCd() {
		return countyCd;
	}
	public void setCountyCd(String countyCd) {
		this.countyCd = countyCd;
	}
	public PGgeometry getGeom() {
		return geom;
	}
	public void setGeom(PGgeometry geom) {
		this.geom = geom;
	}
	public String getStationNm() {
		return stationNm;
	}
	public void setStationNm(String stationNm) {
		this.stationNm = stationNm;
	}
	public String getOrganizationName() {
		return organizationName;
	}
	public void setOrganizationName(String organizationName) {
		this.organizationName = organizationName;
	}
	public String getStationTypeName() {
		return stationTypeName;
	}
	public void setStationTypeName(String stationTypeName) {
		this.stationTypeName = stationTypeName;
	}
	public BigDecimal getDecLatVa() {
		return decLatVa;
	}
	public void setDecLatVa(BigDecimal decLatVa) {
		this.decLatVa = decLatVa;
	}
	public BigDecimal getDecLongVa() {
		return decLongVa;
	}
	public void setDecLongVa(BigDecimal decLongVa) {
		this.decLongVa = decLongVa;
	}
	public String getMapScaleFc() {
		return mapScaleFc;
	}
	public void setMapScaleFc(String mapScaleFc) {
		this.mapScaleFc = mapScaleFc;
	}
	public String getLatLongMethodDescription() {
		return latLongMethodDescription;
	}
	public void setLatLongMethodDescription(String latLongMethodDescription) {
		this.latLongMethodDescription = latLongMethodDescription;
	}
	public String getDecCoordDatumCd() {
		return decCoordDatumCd;
	}
	public void setDecCoordDatumCd(String decCoordDatumCd) {
		this.decCoordDatumCd = decCoordDatumCd;
	}
	public String getAltDatumCd() {
		return altDatumCd;
	}
	public void setAltDatumCd(String altDatumCd) {
		this.altDatumCd = altDatumCd;
	}
	public String getAltVa() {
		return altVa;
	}
	public void setAltVa(String altVa) {
		this.altVa = altVa;
	}
	public String getAltAcyVa() {
		return altAcyVa;
	}
	public void setAltAcyVa(String altAcyVa) {
		this.altAcyVa = altAcyVa;
	}
	public String getAltitudeMethodDescription() {
		return altitudeMethodDescription;
	}
	public void setAltitudeMethodDescription(String altitudeMethodDescription) {
		this.altitudeMethodDescription = altitudeMethodDescription;
	}
	public String getDrainAreaVa() {
		return drainAreaVa;
	}
	public void setDrainAreaVa(String drainAreaVa) {
		this.drainAreaVa = drainAreaVa;
	}
	public String getContribDrainAreaVa() {
		return contribDrainAreaVa;
	}
	public void setContribDrainAreaVa(String contribDrainAreaVa) {
		this.contribDrainAreaVa = contribDrainAreaVa;
	}
	public String getLatLongAccuracy() {
		return latLongAccuracy;
	}
	public void setLatLongAccuracy(String latLongAccuracy) {
		this.latLongAccuracy = latLongAccuracy;
	}
	public String getLatLongAccuracyUnit() {
		return latLongAccuracyUnit;
	}
	public void setLatLongAccuracyUnit(String latLongAccuracyUnit) {
		this.latLongAccuracyUnit = latLongAccuracyUnit;
	}
	public String getNatAqfrName() {
		return natAqfrName;
	}
	public void setNatAqfrName(String natAqfrName) {
		this.natAqfrName = natAqfrName;
	}
	public String getAqfrNm() {
		return aqfrNm;
	}
	public void setAqfrNm(String aqfrNm) {
		this.aqfrNm = aqfrNm;
	}
	public String getAquiferTypeDescription() {
		return aquiferTypeDescription;
	}
	public void setAquiferTypeDescription(String aquiferTypeDescription) {
		this.aquiferTypeDescription = aquiferTypeDescription;
	}
	public String getConstructionDt() {
		return constructionDt;
	}
	public void setConstructionDate(String constructionDt) {
		this.constructionDt = constructionDt;
	}
	public String getWellDepthVa() {
		return wellDepthVa;
	}
	public void setWellDepthVa(String wellDepthVa) {
		this.wellDepthVa = wellDepthVa;
	}
	public String getHoleDepthVa() {
		return holeDepthVa;
	}
	public void setHoleDepthVa(String holeDepthVa) {
		this.holeDepthVa = holeDepthVa;
	}
	
	
}
