package gov.acwi.wqp.etl.mysqlnwis.sitefile;

import java.util.Date;

public class Sitefile {
	
	private int siteId;
    private String agencyCd;
    private String siteNo;
    private String nwisHost;
    private String dbNo;
    private String stationNm;
    private float decLatVa;
    private float decLongVa ;
    private String coordMethCd;
    private String coordAcyCd;
    private String districtCd;
    private String countryCd ;
    private String stateCd;
    private String countyCd;
    private String landNetDs;
    private String mapScaleFc;
    private String altVa;
    private String altMethCd;
    private String altAcyVa;
    private String altDatumCd;
    private String hucCd;
    private String basinCd;
    private String siteTpCd;
    private String siteRmksTx;
    private String drainAreaVa;
    private String contribDrainAreaVa;
    private String constructionDt;
    private String aqfrTypeCd;
    private String aqfrCd;
    private String natAqfrCd;
    private String wellDepthVa;
    private String holeDepthVa;
    private String siteWebCd;
    private String decCoordDatumCd ;
    private String siteCn;
    private Date siteCr;
    private String siteMn;
    private Date siteMd;
    
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
	public String getNwisHost() {
		return nwisHost;
	}
	public void setNwisHost(String nwisHost) {
		this.nwisHost = nwisHost;
	}
	public String getDbNo() {
		return dbNo;
	}
	public void setDbNo(String dbNo) {
		this.dbNo = dbNo;
	}
	public String getStationNm() {
		return stationNm;
	}
	public void setStationNm(String stationNm) {
		this.stationNm = stationNm;
	}
	public float getDecLatVa() {
		return decLatVa;
	}
	public void setDecLatVa(float decLatVa) {
		this.decLatVa = decLatVa;
	}
	public float getDecLongVa() {
		return decLongVa;
	}
	public void setDecLongVa(float decLongVa) {
		this.decLongVa = decLongVa;
	}
	public String getCoordMethCd() {
		return coordMethCd;
	}
	public void setCoordMethCd(String coordMethCd) {
		this.coordMethCd = coordMethCd;
	}
	public String getCoordAcyCd() {
		return coordAcyCd;
	}
	public void setCoordAcyCd(String coordAcyCd) {
		this.coordAcyCd = coordAcyCd;
	}
	public String getDistrictCd() {
		return districtCd;
	}
	public void setDistrictCd(String districtCd) {
		this.districtCd = districtCd;
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
	public String getLandNetDs() {
		return landNetDs;
	}
	public void setLandNetDs(String landNetDs) {
		this.landNetDs = landNetDs;
	}
	public String getMapScaleFc() {
		return mapScaleFc;
	}
	public void setMapScaleFc(String mapScaleFc) {
		this.mapScaleFc = mapScaleFc;
	}
	public String getAltVa() {
		return altVa;
	}
	public void setAltVa(String altVa) {
		this.altVa = altVa;
	}
	public String getAltMethCd() {
		return altMethCd;
	}
	public void setAltMethCd(String altMethCd) {
		this.altMethCd = altMethCd;
	}
	public String getAltAcyVa() {
		return altAcyVa;
	}
	public void setAltAcyVa(String altAcyVa) {
		this.altAcyVa = altAcyVa;
	}
	public String getAltDatumCd() {
		return altDatumCd;
	}
	public void setAltDatumCd(String altDatumCd) {
		this.altDatumCd = altDatumCd;
	}
	public String getHucCd() {
		return hucCd;
	}
	public void setHucCd(String huc_Cd) {
		this.hucCd = huc_Cd;
	}
	public String getBasinCd() {
		return basinCd;
	}
	public void setBasinCd(String basinCd) {
		this.basinCd = basinCd;
	}
	public String getSiteTpCd() {
		return siteTpCd;
	}
	public void setSiteTpCd(String siteTpCd) {
		this.siteTpCd = siteTpCd;
	}
	public String getSiteRmksTx() {
		return siteRmksTx;
	}
	public void setSiteRmksTx(String siteRmksTx) {
		this.siteRmksTx = siteRmksTx;
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
	public String getConstructionDt() {
		return constructionDt;
	}
	public void setConstructionDt(String constructionDt) {
		this.constructionDt = constructionDt;
	}
	public String getAqfrTypeCd() {
		return aqfrTypeCd;
	}
	public void setAqfrTypeCd(String aqfrTypeCd) {
		this.aqfrTypeCd = aqfrTypeCd;
	}
	public String getAqfrCd() {
		return aqfrCd;
	}
	public void setAqfrCd(String aqfrCd) {
		this.aqfrCd = aqfrCd;
	}
	public String getNatAqfrCd() {
		return natAqfrCd;
	}
	public void setNatAqfrCd(String natAqfrCd) {
		this.natAqfrCd = natAqfrCd;
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
	public String getSiteWebCd() {
		return siteWebCd;
	}
	public void setSiteWebCd(String siteWebCd) {
		this.siteWebCd = siteWebCd;
	}
	public String getDecCoordDatumCd() {
		return decCoordDatumCd;
	}
	public void setDecCoordDatumCd(String decCoordDatumCd) {
		this.decCoordDatumCd = decCoordDatumCd;
	}
	public String getSiteCn() {
		return siteCn;
	}
	public void setSiteCn(String siteCn) {
		this.siteCn = siteCn;
	}
	public Date getSiteCr() {
		return siteCr;
	}
	public void setSiteCr(Date siteCr) {
		this.siteCr = siteCr;
	}
	public String getSiteMn() {
		return siteMn;
	}
	public void setSiteMn(String siteMn) {
		this.siteMn = siteMn;
	}
	public Date getSiteMd() {
		return siteMd;
	}
	public void setSiteMd(Date siteMd) {
		this.siteMd = siteMd;
	}
}
