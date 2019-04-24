package gov.acwi.wqp.etl.nwis;

import java.time.LocalDate;

public class NwisDistrictCdsByHost {
	
	private String hostName;
	private String districtCd;
	private String stateName;
	private String statePostalCd;
	private LocalDate lastUpdDate;
	
	public String getHostName() {
		return hostName;
	}
	public void setHostName(String hostName) {
		this.hostName = hostName;
	}
	public String getDistrictCd() {
		return districtCd;
	}
	public void setDistrictCd(String districtCd) {
		this.districtCd = districtCd;
	}
	public String getStateName() {
		return stateName;
	}
	public void setStateName(String stateName) {
		this.stateName = stateName;
	}
	public String getStatePostalCd() {
		return statePostalCd;
	}
	public void setStatePostalCd(String statePostalCd) {
		this.statePostalCd = statePostalCd;
	}
	public LocalDate getLastUpdDate() {
		return lastUpdDate;
	}
	public void setLastUpdDate(LocalDate lastUpdDate) {
		this.lastUpdDate = lastUpdDate;
	}
}
