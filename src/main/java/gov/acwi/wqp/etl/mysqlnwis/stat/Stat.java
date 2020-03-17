package gov.acwi.wqp.etl.mysqlnwis.stat;

public class Stat {
	private String statCd;
	private boolean dvValidFg;
	private boolean uvValidFg;
	private String statNm;
	private String statDs;
	public String getStatCd() {
		return statCd;
	}
	public void setStatCd(String statCd) {
		this.statCd = statCd;
	}
	public boolean getDvValidFg() {
		return dvValidFg;
	}
	public void setDvValidFg(boolean dvValidFg) {
		this.dvValidFg = dvValidFg;
	}
	public boolean getUvValidFg() {
		return uvValidFg;
	}
	public void setUvValidFg(boolean uvValidFg) {
		this.uvValidFg = uvValidFg;
	}
	public String getStatNm() {
		return statNm;
	}
	public void setStatNm(String statNm) {
		this.statNm = statNm;
	}
	public String getStatDs() {
		return statDs;
	}
	public void setStatDs(String statDs) {
		this.statDs = statDs;
	}
}
