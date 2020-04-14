package gov.acwi.wqp.etl.mysqlnwis.gwlevels;

import java.sql.Timestamp;

public class GwLevels {
	private int siteId;
	private String levStrDt;
	private Timestamp levDtm;
	private String levDtAcyCd;
	private String levTzCd;
	private String levTzOffset;
	private String levVa;
	private String levAcyCd;
	private String parameterCd;
	private String levDatumCd;
	private String levSrcCd;
	private String levStatusCd;
	private String levMethCd;
	private String levAgencyCd;
	private String levAgeCd;
	private Timestamp gwLevelsMd;

	public int getSiteId() {
		return siteId;
	}
	public void setSiteId(int siteId) {
		this.siteId = siteId;
	}
	public String getLevStrDt() {
		return levStrDt;
	}
	public void setLevStrDt(String levStrDt) {
		this.levStrDt = levStrDt;
	}
	public Timestamp getLevDtm() {
		return levDtm;
	}
	public void setLevDtm(Timestamp levDtm) {
		this.levDtm = levDtm;
	}
	public String getLevDtAcyCd() {
		return levDtAcyCd;
	}
	public void setLevDtAcyCd(String levDtAcyCd) {
		this.levDtAcyCd = levDtAcyCd;
	}
	public String getLevTzCd() {
		return levTzCd;
	}
	public void setLevTzCd(String levTzCd) {
		this.levTzCd = levTzCd;
	}
	public String getLevTzOffset() {
		return levTzOffset;
	}
	public void setLevTzOffset(String levTzOffset) {
		this.levTzOffset = levTzOffset;
	}
	public String getLevVa() {
		return levVa;
	}
	public void setLevVa(String levVa) {
		this.levVa = levVa;
	}
	public String getLevAcyCd() {
		return levAcyCd;
	}
	public void setLevAcyCd(String levAcyCd) {
		this.levAcyCd = levAcyCd;
	}
	public String getParameterCd() {
		return parameterCd;
	}
	public void setParameterCd(String parameterCd) {
		this.parameterCd = parameterCd;
	}
	public String getLevDatumCd() {
		return levDatumCd;
	}
	public void setLevDatumCd(String levDatumCd) {
		this.levDatumCd = levDatumCd;
	}
	public String getLevSrcCd() {
		return levSrcCd;
	}
	public void setLevSrcCd(String levSrcCd) {
		this.levSrcCd = levSrcCd;
	}
	public String getLevStatusCd() {
		return levStatusCd;
	}
	public void setLevStatusCd(String levStatusCd) {
		this.levStatusCd = levStatusCd;
	}
	public String getLevMethCd() {
		return levMethCd;
	}
	public void setLevMethCd(String levMethCd) {
		this.levMethCd = levMethCd;
	}
	public String getLevAgencyCd() {
		return levAgencyCd;
	}
	public void setLevAgencyCd(String levAgencyCd) {
		this.levAgencyCd = levAgencyCd;
	}
	public String getLevAgeCd() {
		return levAgeCd;
	}
	public void setLevAgeCd(String levAgeCd) {
		this.levAgeCd = levAgeCd;
	}
	public Timestamp getGwLevelsMd() {
		return gwLevelsMd;
	}
	public void setGwLevelsMd(Timestamp gwLevelsMd) {
		this.gwLevelsMd = gwLevelsMd;
	}
}
