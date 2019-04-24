package gov.acwi.wqp.etl.nwis;

import java.time.LocalDate;

import org.postgis.PGgeometry;

public class NwisResult {
	
	private int stationId;
	private String siteId;
	private LocalDate eventDate;
	private String nemiUrl;
	private String parameterCd;
	private String activity;
	private String wqpcrosswalk;
	private String srsname;
	private String parmSeqGrpNm;
	private String sampleMedia;
	private String organization;
	private String siteType;
	private String huc;
	private String governmentalUnitCode;
	private PGgeometry geom;
	private String organizationName;
	private int activityId;
	private String activityTypeCode;
	private String activityMediaSubdivName;
	private String activityStartTime;
	private String actStartTimeZone;
	private String activityStopDate;
	private String activityStopTime;
	private String actStopTimeZone;
	private String activityDepth;
	private String activityDepthUnit;
	private String activityDepthRefPoint;
	private String activityUpperDepth;
	private String activityUpperDepthUnit;
	private String activityLowerDepth;
	private String activityLowerDepthUnit;
	private String projectId;
	private String activityConductingOrg;
	private String activityComment;
	private String sampleAqfrName;
	private String hydrologicConditionName;
	private String hydrologicEventName;
	private String sampleCollectMethodId;
	private String sampleCollectMethodCtx;
	private String sampleCollectMethodName;
	private String sampleCollectEquipName;
	private int resultId;
	private String remarkCd;
	private String parmFracTx;
	private String fxdTx;
	private String resultVa;
	private String parmUntTx;
	private String dqiCd;
	private String parmStatTx;
	private String resultMd;
	private String parmWtTx;
	private String parmTmTx;
	private String parmTempTx;
	private String parmSizeTx;
	private String labStdVa;
	private String resultLabCmTx;
	private String parmMediumTx;
	private String sampleTissueTaxonomicName;
	private String sampleTissueAnatomyName;
	private String methCd;
	private String methNm;
	private String citNm;
	private String protoOrgNm;
	private String anlDt;
	private String valQualCd1Nm;
	private String valQualCd2Nm;
	private String valQualCd3Nm;
	private String valQualCd4Nm;
	private String valQualCd5Nm;
	private String rptLevVa;
	private String resultUnrndVa;
	private String parmMethMultiplier;
	private String parmMultiplier;
	private String rptLevCd;
	private String rptLevNm;
	private String prepDt;
	
	public int getStationId() {
		return stationId;
	}
	public void setStationId(int stationId) {
		this.stationId = stationId;
	}
	public String getSiteId() {
		return siteId;
	}
	public void setSiteId(String siteId) {
		this.siteId = siteId;
	}
	public LocalDate getEventDate() {
		return eventDate;
	}
	public void setEventDate(LocalDate eventDate) {
		this.eventDate = eventDate;
	}
	public String getNemiUrl() {
		return nemiUrl;
	}
	public void setNemiUrl(String nemiUrl) {
		this.nemiUrl = nemiUrl;
	}
	public String getParameterCd() {
		return parameterCd;
	}
	public void setParameterCd(String parameterCd) {
		this.parameterCd = parameterCd;
	}
	public String getActivity() {
		return activity;
	}
	public void setActivity(String activity) {
		this.activity = activity;
	}
	public String getWqpcrosswalk() {
		return wqpcrosswalk;
	}
	public void setWqpcrosswalk(String wqpcrosswalk) {
		this.wqpcrosswalk = wqpcrosswalk;
	}
	public String getSrsname() {
		return srsname;
	}
	public void setSrsname(String srsname) {
		this.srsname = srsname;
	}
	public String getParmSeqGrpNm() {
		return parmSeqGrpNm;
	}
	public void setParmSeqGrpNm(String parmSeqGrpNm) {
		this.parmSeqGrpNm = parmSeqGrpNm;
	}
	public String getSampleMedia() {
		return sampleMedia;
	}
	public void setSampleMedia(String sampleMedia) {
		this.sampleMedia = sampleMedia;
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
	public int getActivityId() {
		return activityId;
	}
	public void setActivityId(int activityId) {
		this.activityId = activityId;
	}
	public String getActivityTypeCode() {
		return activityTypeCode;
	}
	public void setActivityTypeCode(String activityTypeCode) {
		this.activityTypeCode = activityTypeCode;
	}
	public String getActivityMediaSubdivName() {
		return activityMediaSubdivName;
	}
	public void setActivityMediaSubdivName(String activityMediaSubdivName) {
		this.activityMediaSubdivName = activityMediaSubdivName;
	}
	public String getActivityStartTime() {
		return activityStartTime;
	}
	public void setActivityStartTime(String activityStartTime) {
		this.activityStartTime = activityStartTime;
	}
	public String getActStartTimeZone() {
		return actStartTimeZone;
	}
	public void setActStartTimeZone(String actStartTimeZone) {
		this.actStartTimeZone = actStartTimeZone;
	}
	public String getActivityStopDate() {
		return activityStopDate;
	}
	public void setActivityStopDate(String activityStopDate) {
		this.activityStopDate = activityStopDate;
	}
	public String getActivityStopTime() {
		return activityStopTime;
	}
	public void setActivityStopTime(String activityStopTime) {
		this.activityStopTime = activityStopTime;
	}
	public String getActStopTimeZone() {
		return actStopTimeZone;
	}
	public void setActStopTimeZone(String actStopTimeZone) {
		this.actStopTimeZone = actStopTimeZone;
	}
	public String getActivityDepth() {
		return activityDepth;
	}
	public void setActivityDepth(String activityDepth) {
		this.activityDepth = activityDepth;
	}
	public String getActivityDepthUnit() {
		return activityDepthUnit;
	}
	public void setActivityDepthUnit(String activityDepthUnit) {
		this.activityDepthUnit = activityDepthUnit;
	}
	public String getActivityDepthRefPoint() {
		return activityDepthRefPoint;
	}
	public void setActivityDepthRefPoint(String activityDepthRefPoint) {
		this.activityDepthRefPoint = activityDepthRefPoint;
	}
	public String getActivityUpperDepth() {
		return activityUpperDepth;
	}
	public void setActivityUpperDepth(String activityUpperDepth) {
		this.activityUpperDepth = activityUpperDepth;
	}
	public String getActivityUpperDepthUnit() {
		return activityUpperDepthUnit;
	}
	public void setActivityUpperDepthUnit(String activityUpperDepthUnit) {
		this.activityUpperDepthUnit = activityUpperDepthUnit;
	}
	public String getActivityLowerDepth() {
		return activityLowerDepth;
	}
	public void setActivityLowerDepth(String activityLowerDepth) {
		this.activityLowerDepth = activityLowerDepth;
	}
	public String getActivityLowerDepthUnit() {
		return activityLowerDepthUnit;
	}
	public void setActivityLowerDepthUnit(String activityLowerDepthUnit) {
		this.activityLowerDepthUnit = activityLowerDepthUnit;
	}
	public String getProjectId() {
		return projectId;
	}
	public void setProjectId(String projectId) {
		this.projectId = projectId;
	}
	public String getActivityConductingOrg() {
		return activityConductingOrg;
	}
	public void setActivityConductingOrg(String activityConductingOrg) {
		this.activityConductingOrg = activityConductingOrg;
	}
	public String getActivityComment() {
		return activityComment;
	}
	public void setActivityComment(String activityComment) {
		this.activityComment = activityComment;
	}
	public String getSampleAqfrName() {
		return sampleAqfrName;
	}
	public void setSampleAqfrName(String sampleAqfrName) {
		this.sampleAqfrName = sampleAqfrName;
	}
	public String getHydrologicConditionName() {
		return hydrologicConditionName;
	}
	public void setHydrologicConditionName(String hydrologicConditionName) {
		this.hydrologicConditionName = hydrologicConditionName;
	}
	public String getHydrologicEventName() {
		return hydrologicEventName;
	}
	public void setHydrologicEventName(String hydrologicEventName) {
		this.hydrologicEventName = hydrologicEventName;
	}
	public String getSampleCollectMethodId() {
		return sampleCollectMethodId;
	}
	public void setSampleCollectMethodId(String sampleCollectMethodId) {
		this.sampleCollectMethodId = sampleCollectMethodId;
	}
	public String getSampleCollectMethodCtx() {
		return sampleCollectMethodCtx;
	}
	public void setSampleCollectMethodCtx(String sampleCollectMethodCtx) {
		this.sampleCollectMethodCtx = sampleCollectMethodCtx;
	}
	public String getSampleCollectMethodName() {
		return sampleCollectMethodName;
	}
	public void setSampleCollectMethodName(String sampleCollectMethodName) {
		this.sampleCollectMethodName = sampleCollectMethodName;
	}
	public String getSampleCollectEquipName() {
		return sampleCollectEquipName;
	}
	public void setSampleCollectEquipName(String sampleCollectEquipName) {
		this.sampleCollectEquipName = sampleCollectEquipName;
	}
	public int getResultId() {
		return resultId;
	}
	public void setResultId(int resultId) {
		this.resultId = resultId;
	}
	public String getRemarkCd() {
		return remarkCd;
	}
	public void setRemarkCd(String remarkCd) {
		this.remarkCd = remarkCd;
	}
	public String getParmFracTx() {
		return parmFracTx;
	}
	public void setParmFracTx(String parmFracTx) {
		this.parmFracTx = parmFracTx;
	}
	public String getFxdTx() {
		return fxdTx;
	}
	public void setFxdTx(String fxdTx) {
		this.fxdTx = fxdTx;
	}
	public String getResultVa() {
		return resultVa;
	}
	public void setResultVa(String resultVa) {
		this.resultVa = resultVa;
	}
	public String getParmUntTx() {
		return parmUntTx;
	}
	public void setParmUntTx(String parmUntTx) {
		this.parmUntTx = parmUntTx;
	}
	public String getDqiCd() {
		return dqiCd;
	}
	public void setDqiCd(String dqiCd) {
		this.dqiCd = dqiCd;
	}
	public String getParmStatTx() {
		return parmStatTx;
	}
	public void setParmStatTx(String parmStatTx) {
		this.parmStatTx = parmStatTx;
	}
	public String getResultMd() {
		return resultMd;
	}
	public void setResultMd(String resultMd) {
		this.resultMd = resultMd;
	}
	public String getParmWtTx() {
		return parmWtTx;
	}
	public void setParmWtTx(String parmWtTx) {
		this.parmWtTx = parmWtTx;
	}
	public String getParmTmTx() {
		return parmTmTx;
	}
	public void setParmTmTx(String parmTmTx) {
		this.parmTmTx = parmTmTx;
	}
	public String getParmTempTx() {
		return parmTempTx;
	}
	public void setParmTempTx(String parmTempTx) {
		this.parmTempTx = parmTempTx;
	}
	public String getParmSizeTx() {
		return parmSizeTx;
	}
	public void setParmSizeTx(String parmSizeTx) {
		this.parmSizeTx = parmSizeTx;
	}
	public String getLabStdVa() {
		return labStdVa;
	}
	public void setLabStdVa(String labStdVa) {
		this.labStdVa = labStdVa;
	}
	public String getResultLabCmTx() {
		return resultLabCmTx;
	}
	public void setResultLabCmTx(String resultLabCmTx) {
		this.resultLabCmTx = resultLabCmTx;
	}
	public String getParmMediumTx() {
		return parmMediumTx;
	}
	public void setParmMediumTx(String parmMediumTx) {
		this.parmMediumTx = parmMediumTx;
	}
	public String getSampleTissueTaxonomicName() {
		return sampleTissueTaxonomicName;
	}
	public void setSampleTissueTaxonomicName(String sampleTissueTaxonomicName) {
		this.sampleTissueTaxonomicName = sampleTissueTaxonomicName;
	}
	public String getSampleTissueAnatomyName() {
		return sampleTissueAnatomyName;
	}
	public void setSampleTissueAnatomyName(String sampleTissueAnatomyName) {
		this.sampleTissueAnatomyName = sampleTissueAnatomyName;
	}
	public String getMethCd() {
		return methCd;
	}
	public void setMethCd(String methCd) {
		this.methCd = methCd;
	}
	public String getMethNm() {
		return methNm;
	}
	public void setMethNm(String methNm) {
		this.methNm = methNm;
	}
	public String getCitNm() {
		return citNm;
	}
	public void setCitNm(String citNm) {
		this.citNm = citNm;
	}
	public String getProtoOrgNm() {
		return protoOrgNm;
	}
	public void setProtoOrgNm(String protoOrgNm) {
		this.protoOrgNm = protoOrgNm;
	}
	public String getAnlDt() {
		return anlDt;
	}
	public void setAnlDt(String anlDt) {
		this.anlDt = anlDt;
	}
	public String getValQualCd1Nm() {
		return valQualCd1Nm;
	}
	public void setValQualCd1Nm(String valQualCd1Nm) {
		this.valQualCd1Nm = valQualCd1Nm;
	}
	public String getValQualCd2Nm() {
		return valQualCd2Nm;
	}
	public void setValQualCd2Nm(String valQualCd2Nm) {
		this.valQualCd2Nm = valQualCd2Nm;
	}
	public String getValQualCd3Nm() {
		return valQualCd3Nm;
	}
	public void setValQualCd3Nm(String valQualCd3Nm) {
		this.valQualCd3Nm = valQualCd3Nm;
	}
	public String getValQualCd4Nm() {
		return valQualCd4Nm;
	}
	public void setValQualCd4Nm(String valQualCd4Nm) {
		this.valQualCd4Nm = valQualCd4Nm;
	}
	public String getValQualCd5Nm() {
		return valQualCd5Nm;
	}
	public void setValQualCd5Nm(String valQualCd5Nm) {
		this.valQualCd5Nm = valQualCd5Nm;
	}
	public String getRptLevVa() {
		return rptLevVa;
	}
	public void setRptLevVa(String rptLevVa) {
		this.rptLevVa = rptLevVa;
	}
	public String getResultUnrndVa() {
		return resultUnrndVa;
	}
	public void setResultUnrndVa(String resultUnrndVa) {
		this.resultUnrndVa = resultUnrndVa;
	}
	public String getParmMethMultiplier() {
		return parmMethMultiplier;
	}
	public void setParmMethMultiplier(String parmMethMultiplier) {
		this.parmMethMultiplier = parmMethMultiplier;
	}
	public String getParmMultiplier() {
		return parmMultiplier;
	}
	public void setParmMultiplier(String parmMultiplier) {
		this.parmMultiplier = parmMultiplier;
	}
	public String getRptLevCd() {
		return rptLevCd;
	}
	public void setRptLevCd(String rptLevCd) {
		this.rptLevCd = rptLevCd;
	}
	public String getRptLevNm() {
		return rptLevNm;
	}
	public void setRptLevNm(String rptLevNm) {
		this.rptLevNm = rptLevNm;
	}
	public String getPrepDt() {
		return prepDt;
	}
	public void setPrepDt(String prepDt) {
		this.prepDt = prepDt;
	}
}
