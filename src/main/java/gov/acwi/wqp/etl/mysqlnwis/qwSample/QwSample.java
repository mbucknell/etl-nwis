package gov.acwi.wqp.etl.mysqlnwis.qwSample;

import java.time.LocalDate;
import java.time.LocalDateTime;

public class QwSample {

	private int sampleId;
	private int siteId;
	private String recordNo;
	private String nwisHost;
	private String dbNo;
	private String qwDbNo;
	private boolean sampleWebCd;
	private LocalDateTime sampleStartDt;
	private String sampleStartDisplayDt;
	private String sampleStartSg;
	private String sampleEndDt;
	private String sampleEndDisplayDt;
	private String sampleEndSg;
	private LocalDateTime sampleUtcStartDt;
	private String sampleUtcStartDisplayDt;
	private String sampleUtcEndDt;
	private String sampleUtcEndDisplayDt;
	private String sampleStartTimeDatumCd;
	private String mediumCd;
	private String tuId;
	private String bodyPartId;
	private String nwisSampleId;
	private String labNo;
	private String projectCd;
	private String aqfrCd;
	private String sampTypeCd;
	private String sampleLabCmTx;
	private String sampleFieldCmTx;
	private String collEntCd;
	private String anlStatCd;
	private String anlSrcCd;
	private String anlTypeTx;
	private String hydCondCd;
	private String hydEventCd;
	private String tmDatumRlbtyCd;
	private LocalDate sampleMd;
	private LocalDate qwSampleMd;
	
	public int getSampleId() {
		return sampleId;
	}
	public void setSampleId(int sampleId) {
		this.sampleId = sampleId;
	}
	public int getSiteId() {
		return siteId;
	}
	public void setSiteId(int siteId) {
		this.siteId = siteId;
	}
	public String getRecordNo() {
		return recordNo;
	}
	public void setRecordNo(String recordNo) {
		this.recordNo = recordNo;
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
	public String getQwDbNo() {
		return qwDbNo;
	}
	public void setQwDbNo(String qwDbNo) {
		this.qwDbNo = qwDbNo;
	}
	public boolean isSampleWebCd() {
		return sampleWebCd;
	}
	public void setSampleWebCd(boolean sampleWebCd) {
		this.sampleWebCd = sampleWebCd;
	}
	public LocalDateTime getSampleStartDt() {
		return sampleStartDt;
	}
	public void setSampleStartDt(LocalDateTime sampleStartDt) {
		this.sampleStartDt = sampleStartDt;
	}
	public String getSampleStartDisplayDt() {
		return sampleStartDisplayDt;
	}
	public void setSampleStartDisplayDt(String sampleStartDisplayDt) {
		this.sampleStartDisplayDt = sampleStartDisplayDt;
	}
	public String getSampleStartSg() {
		return sampleStartSg;
	}
	public void setSampleStartSg(String sampleStartSg) {
		this.sampleStartSg = sampleStartSg;
	}
	public String getSampleEndDt() {
		return sampleEndDt;
	}
	public void setSampleEndDt(String sampleEndDt) {
		this.sampleEndDt = sampleEndDt;
	}
	public String getSampleEndDisplayDt() {
		return sampleEndDisplayDt;
	}
	public void setSampleEndDisplayDt(String sampleEndDisplayDt) {
		this.sampleEndDisplayDt = sampleEndDisplayDt;
	}
	public String getSampleEndSg() {
		return sampleEndSg;
	}
	public void setSampleEndSg(String sampleEndSg) {
		this.sampleEndSg = sampleEndSg;
	}
	public LocalDateTime getSampleUtcStartDt() {
		return sampleUtcStartDt;
	}
	public void setSampleUtcStartDt(LocalDateTime sampleUtcStartDt) {
		this.sampleUtcStartDt = sampleUtcStartDt;
	}
	public String getSampleUtcStartDisplayDt() {
		return sampleUtcStartDisplayDt;
	}
	public void setSampleUtcStartDisplayDt(String sampleUtcStartDisplayDt) {
		this.sampleUtcStartDisplayDt = sampleUtcStartDisplayDt;
	}
	
	public String getSampleUtcEndDt() {
		return sampleUtcEndDt;
	}
	public void setSampleUtcEndDt(String sampleUtcEndDt) {
		this.sampleUtcEndDt = sampleUtcEndDt;
	}
	public String getSampleUtcEndDisplayDt() {
		return sampleUtcEndDisplayDt;
	}
	public void setSampleUtcEndDisplayDt(String sampleUtcEndDisplayDt) {
		this.sampleUtcEndDisplayDt = sampleUtcEndDisplayDt;
	}
	public String getSampleStartTimeDatumCd() {
		return sampleStartTimeDatumCd;
	}
	public void setSampleStartTimeDatumCd(String sampleStartTimeDatumCd) {
		this.sampleStartTimeDatumCd = sampleStartTimeDatumCd;
	}
	public String getMediumCd() {
		return mediumCd;
	}
	public void setMediumCd(String mediumCd) {
		this.mediumCd = mediumCd;
	}
	public String getTuId() {
		return tuId;
	}
	public void setTuId(String tuId) {
		this.tuId = tuId;
	}
	public String getBodyPartId() {
		return bodyPartId;
	}
	public void setBodyPartId(String bodyPartId) {
		this.bodyPartId = bodyPartId;
	}
	
	public String getNwisSampleId() {
		return nwisSampleId;
	}
	public void setNwisSampleId(String nwisSampleId) {
		this.nwisSampleId = nwisSampleId;
	}
	public String getLabNo() {
		return labNo;
	}
	public void setLabNo(String labNo) {
		this.labNo = labNo;
	}
	public String getProjectCd() {
		return projectCd;
	}
	public void setProjectCd(String projectCd) {
		this.projectCd = projectCd;
	}
	public String getAqfrCd() {
		return aqfrCd;
	}
	public void setAqfrCd(String aqfrCd) {
		this.aqfrCd = aqfrCd;
	}
	public String getSampTypeCd() {
		return sampTypeCd;
	}
	public void setSampTypeCd(String sampTypeCd) {
		this.sampTypeCd = sampTypeCd;
	}
	public String getSampleLabCmTx() {
		return sampleLabCmTx;
	}
	public void setSampleLabCmTx(String sampleLabCmTx) {
		this.sampleLabCmTx = sampleLabCmTx;
	}
	public String getSampleFieldCmTx() {
		return sampleFieldCmTx;
	}
	public void setSampleFieldCmTx(String sampleFieldCmTx) {
		this.sampleFieldCmTx = sampleFieldCmTx;
	}
	public String getCollEntCd() {
		return collEntCd;
	}
	public void setCollEntCd(String collEntCd) {
		this.collEntCd = collEntCd;
	}
	public String getAnlStatCd() {
		return anlStatCd;
	}
	public void setAnlStatCd(String anlStatCd) {
		this.anlStatCd = anlStatCd;
	}
	public String getAnlSrcCd() {
		return anlSrcCd;
	}
	public void setAnlSrcCd(String anlSrcCd) {
		this.anlSrcCd = anlSrcCd;
	}
	public String getAnlTypeTx() {
		return anlTypeTx;
	}
	public void setAnlTypeTx(String anlTypeTx) {
		this.anlTypeTx = anlTypeTx;
	}
	public String getHydCondCd() {
		return hydCondCd;
	}
	public void setHydCondCd(String hydCondCd) {
		this.hydCondCd = hydCondCd;
	}
	public String getHydEventCd() {
		return hydEventCd;
	}
	public void setHydEventCd(String hydEventCd) {
		this.hydEventCd = hydEventCd;
	}
	public String getTmDatumRlbtyCd() {
		return tmDatumRlbtyCd;
	}
	public void setTmDatumRlbtyCd(String tmDatumRlbtyCd) {
		this.tmDatumRlbtyCd = tmDatumRlbtyCd;
	}
	public LocalDate getSampleMd() {
		return sampleMd;
	}
	public void setSampleMd(LocalDate sampleMd) {
		this.sampleMd = sampleMd;
	}
	public LocalDate getQwSampleMd() {
		return qwSampleMd;
	}
	public void setQwSampleMd(LocalDate qwSampleMd) {
		this.qwSampleMd = qwSampleMd;
	}
}
