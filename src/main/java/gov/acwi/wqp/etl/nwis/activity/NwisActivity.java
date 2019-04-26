package gov.acwi.wqp.etl.nwis.activity;

import java.time.LocalDateTime;

import org.postgis.PGgeometry;

public class NwisActivity {

    private Integer stationId;
    private String siteId;
    private LocalDateTime sampleStartDt;
    private String nwisHost;
    private String qwDbNo;
    private String recordNo;
    private String wqxActMedNm;
    private String organization;
    private String siteType;
    private String huc;
    private String governmentalUnitCode;
    private PGgeometry geom;
    private String organizationName;
    private Integer sampleId;
    private String sampTypeCd;
    private String wqxActMedSub;
    private String sampleStartSg;
    private String sampleStartTimeDatumCd;
    private String sampleEndSg;
    private String sampleEndDt;
    private String v00003;
    private String v00098;
    private String v50280;
    private String v71999;
    private String v72015;
    private String v72016;
    private String v78890;
    private String v78891;
    private String v82047;
    private String v82048;
    private String v82398;
    private String v82398FxdTx;
    private String v84164FxdTx;
    private String nawqaSiteNo;
    private String protoOrgNm;
    private String collEntCd;
    private String sampleLabCmTx;
    private String aqfrNm;
    private String hydCondNm;
    private String hydEventNm;

    public Integer getStationId() {
        return stationId;
    }

    public void setStationId(Integer stationId) {
        this.stationId = stationId;
    }

    public String getSiteId() {
        return siteId;
    }

    public void setSiteId(String siteId) {
        this.siteId = siteId;
    }

    public LocalDateTime getSampleStartDt() {
        return sampleStartDt;
    }

    public void setSampleStartDt(LocalDateTime sampleStartDt) {
        this.sampleStartDt = sampleStartDt;
    }

    public String getNwisHost() {
        return nwisHost;
    }

    public void setNwisHost(String nwisHost) {
        this.nwisHost = nwisHost;
    }

    public String getQwDbNo() {
        return qwDbNo;
    }

    public void setQwDbNo(String qwDbNo) {
        this.qwDbNo = qwDbNo;
    }

    public String getRecordNo() {
        return recordNo;
    }

    public void setRecordNo(String recordNo) {
        this.recordNo = recordNo;
    }

    public String getWqxActMedNm() {
        return wqxActMedNm;
    }

    public void setWqxActMedNm(String wqxActMedNm) {
        this.wqxActMedNm = wqxActMedNm;
    }

    public String getOrganization() {
        return organization;
    }

    public void setOrganization(String organization) {
        this.organization = organization;
    }

    public String getSiteType() {
        return siteType;
    }

    public void setSiteType(String siteType) {
        this.siteType = siteType;
    }

    public String getHuc() {
        return huc;
    }

    public void setHuc(String huc) {
        this.huc = huc;
    }

    public String getGovernmentalUnitCode() {
        return governmentalUnitCode;
    }

    public void setGovernmentalUnitCode(String governmentalUnitCode) {
        this.governmentalUnitCode = governmentalUnitCode;
    }

    public PGgeometry getGeom() {
        return geom;
    }

    public void setGeom(PGgeometry geom) {
        this.geom = geom;
    }

    public String getOrganizationName() {
        return organizationName;
    }

    public void setOrganizationName(String organizationName) {
        this.organizationName = organizationName;
    }

    public Integer getSampleId() {
        return sampleId;
    }

    public void setSampleId(Integer sampleId) {
        this.sampleId = sampleId;
    }

    public String getSampTypeCd() {
        return sampTypeCd;
    }

    public void setSampTypeCd(String sampTypeCd) {
        this.sampTypeCd = sampTypeCd;
    }

    public String getWqxActMedSub() {
        return wqxActMedSub;
    }

    public void setWqxActMedSub(String wqxActMedSub) {
        this.wqxActMedSub = wqxActMedSub;
    }

    public String getSampleStartSg() {
        return sampleStartSg;
    }

    public void setSampleStartSg(String sampleStartSg) {
        this.sampleStartSg = sampleStartSg;
    }

    public String getSampleStartTimeDatumCd() {
        return sampleStartTimeDatumCd;
    }

    public void setSampleStartTimeDatumCd(String sampleStartTimeDatumCd) {
        this.sampleStartTimeDatumCd = sampleStartTimeDatumCd;
    }

    public String getSampleEndSg() {
        return sampleEndSg;
    }

    public void setSampleEndSg(String sampleEndSg) {
        this.sampleEndSg = sampleEndSg;
    }

    public String getSampleEndDt() {
        return sampleEndDt;
    }

    public void setSampleEndDt(String sampleEndDt) {
        this.sampleEndDt = sampleEndDt;
    }

    public String getV00003() {
        return v00003;
    }

    public void setV00003(String v00003) {
        this.v00003 = v00003;
    }

    public String getV00098() {
        return v00098;
    }

    public void setV00098(String v00098) {
        this.v00098 = v00098;
    }

    public String getV50280() {
        return v50280;
    }

    public void setV50280(String v50280) {
        this.v50280 = v50280;
    }

    public String getV71999() {
        return v71999;
    }

    public void setV71999(String v71999) {
        this.v71999 = v71999;
    }

    public String getV72015() {
        return v72015;
    }

    public void setV72015(String v72015) {
        this.v72015 = v72015;
    }

    public String getV72016() {
        return v72016;
    }

    public void setV72016(String v72016) {
        this.v72016 = v72016;
    }

    public String getV78890() {
        return v78890;
    }

    public void setV78890(String v78890) {
        this.v78890 = v78890;
    }

    public String getV78891() {
        return v78891;
    }

    public void setV78891(String v78891) {
        this.v78891 = v78891;
    }

    public String getV82047() {
        return v82047;
    }

    public void setV82047(String v82047) {
        this.v82047 = v82047;
    }

    public String getV82048() {
        return v82048;
    }

    public void setV82048(String v82048) {
        this.v82048 = v82048;
    }

    public String getV82398() {
        return v82398;
    }

    public void setV82398(String v82398) {
        this.v82398 = v82398;
    }

    public String getV82398FxdTx() {
        return v82398FxdTx;
    }

    public void setV82398FxdTx(String v82398FxdTx) {
        this.v82398FxdTx = v82398FxdTx;
    }

    public String getV84164FxdTx() {
        return v84164FxdTx;
    }

    public void setV84164FxdTx(String v84164FxdTx) {
        this.v84164FxdTx = v84164FxdTx;
    }

    public String getNawqaSiteNo() {
        return nawqaSiteNo;
    }

    public void setNawqaSiteNo(String nawqaSiteNo) {
        this.nawqaSiteNo = nawqaSiteNo;
    }

    public String getProtoOrgNm() {
        return protoOrgNm;
    }

    public void setProtoOrgNm(String protoOrgNm) {
        this.protoOrgNm = protoOrgNm;
    }

    public String getCollEntCd() {
        return collEntCd;
    }

    public void setCollEntCd(String collEntCd) {
        this.collEntCd = collEntCd;
    }

    public String getSampleLabCmTx() {
        return sampleLabCmTx;
    }

    public void setSampleLabCmTx(String sampleLabCmTx) {
        this.sampleLabCmTx = sampleLabCmTx;
    }

    public String getAqfrNm() {
        return aqfrNm;
    }

    public void setAqfrNm(String aqfrNm) {
        this.aqfrNm = aqfrNm;
    }

    public String getHydCondNm() {
        return hydCondNm;
    }

    public void setHydCondNm(String hydCondNm) {
        this.hydCondNm = hydCondNm;
    }

    public String getHydEventNm() {
        return hydEventNm;
    }

    public void setHydEventNm(String hydEventNm) {
        this.hydEventNm = hydEventNm;
    }
}
