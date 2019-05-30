package gov.acwi.wqp.etl.result;

import java.util.AbstractMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import gov.acwi.wqp.etl.ConfigurationService;
import gov.acwi.wqp.etl.nwis.result.NwisResult;

@Component
public class ResultProcessor implements ItemProcessor<NwisResult, Result>{

	private final ConfigurationService configurationService;

	private static final Map<String, String> RESULT_DETECTION_CONDITION_TX = Map.ofEntries(
			new AbstractMap.SimpleEntry<>("U", "Not Detected"),
			new AbstractMap.SimpleEntry<>("V", "Systematic Contamination"),
			new AbstractMap.SimpleEntry<>("M", "Detected Not Quantified"),
			new AbstractMap.SimpleEntry<>("N", "Detected Not Quantified"),
			new AbstractMap.SimpleEntry<>("<", "Not Detected"),
			new AbstractMap.SimpleEntry<>(">", "Present Above Quantification Limit")
			);
	
	private static final Map<String, String> RESULT_VALUE_STATUS = Map.ofEntries(
			new AbstractMap.SimpleEntry<>("S", "Preliminary"),
			new AbstractMap.SimpleEntry<>("A", "Historical"),
			new AbstractMap.SimpleEntry<>("R", "Accepted")
			);
	
	private static final Map<String, String> STATISTIC_TYPE = Map.ofEntries(
			new AbstractMap.SimpleEntry<>("S", "MPN"),
			new AbstractMap.SimpleEntry<>("A", "mean")
			);

	@Autowired
	public ResultProcessor(ConfigurationService configurationService) {
		this.configurationService = configurationService;
	}

	@Override
	public Result process(NwisResult nwisResult) throws Exception {
		Result result = new Result();
		
		String remarkCd = nwisResult.getRemarkCd() == null ? "" : nwisResult.getRemarkCd();
		
		String resultMeasureValue = getResultMeasureValue(
				nwisResult.getFxdTx(), remarkCd, nwisResult.getResultVa());
		String anlDt = nwisResult.getAnlDt();
		String detectionLimit = getDetectionLimit(
				remarkCd, 
				nwisResult.getResultVa(), 
				nwisResult.getRptLevVa(), 
				nwisResult.getResultUnrndVa(), 
				nwisResult.getParmMethMultiplier(), 
				nwisResult.getParmMultiplier(),
				nwisResult.getRptLevCd());
		String prepDt = nwisResult.getPrepDt();
		
		result.setDataSourceId(configurationService.getEtlDataSourceId());
		result.setDataSource(configurationService.getEtlDataSource());
		result.setStationId(nwisResult.getStationId());
		result.setSiteId(nwisResult.getSiteId());
		result.setEventDate(nwisResult.getEventDate());
		result.setAnalyticalMethod(nwisResult.getNemiUrl());
		result.setpCode(nwisResult.getParameterCd());
		result.setActivity(nwisResult.getActivity());
		result.setCharacteristicName(nwisResult.getWqpcrosswalk() != null ? nwisResult.getWqpcrosswalk() : nwisResult.getSrsname());
		result.setCharacteristicType(nwisResult.getParmSeqGrpNm());
		result.setSampleMedia(nwisResult.getSampleMedia());
		result.setOrganization(nwisResult.getOrganization());
		result.setSiteType(nwisResult.getSiteType());
		result.setHuc(nwisResult.getHuc());
		result.setGovernmentalUnitCode(nwisResult.getGovernmentalUnitCode());
		result.setGeom(nwisResult.getGeom());
		result.setOrganizationName(nwisResult.getOrganizationName());
		result.setActivityId(nwisResult.getActivityId());
		result.setActivityTypeCode(nwisResult.getActivityTypeCode());
		result.setActivityMediaSubdivName(nwisResult.getActivityMediaSubdivName());
		result.setActivityStartTime(nwisResult.getActivityStartTime());
		result.setActStartTimeZone(nwisResult.getActStartTimeZone());
		result.setActivityStopDate(nwisResult.getActivityStopDate());
		result.setActivityStopTime(nwisResult.getActivityStopTime());
		result.setActStopTimeZone(nwisResult.getActStopTimeZone());
		result.setActivityDepth(nwisResult.getActivityDepth());
		result.setActivityDepthUnit(nwisResult.getActivityDepthUnit());
		result.setActivityDepthRefPoint(nwisResult.getActivityDepthRefPoint());
		result.setActivityUpperDepth(nwisResult.getActivityUpperDepth());
		result.setActivityUpperDepthUnit(nwisResult.getActivityUpperDepthUnit());
		result.setActivityLowerDepth(nwisResult.getActivityLowerDepth());
		result.setActivityLowerDepthUnit(nwisResult.getActivityLowerDepthUnit());
		result.setProjectId(nwisResult.getProjectId());
		result.setActivityConductingOrg(nwisResult.getActivityConductingOrg());
		result.setActivityComment(nwisResult.getActivityComment());
		result.setSampleAqfrName(nwisResult.getSampleAqfrName());
		result.setHydrologicConditionName(nwisResult.getHydrologicConditionName());
		result.setHydrologicEventName(nwisResult.getHydrologicEventName());
		result.setSampleCollectMethodId(nwisResult.getSampleCollectMethodId());
		result.setSampleCollectMethodCtx(nwisResult.getSampleCollectMethodCtx());
		result.setSampleCollectMethodName(nwisResult.getSampleCollectMethodName());
		result.setSampleCollectEquipName(nwisResult.getSampleCollectEquipName());
		result.setResultId(nwisResult.getResultId());
		result.setResultDetectionConditionTx(RESULT_DETECTION_CONDITION_TX.getOrDefault(remarkCd, null));
		result.setSampleFractionType(nwisResult.getParmFracTx());
		result.setResultMeasureValue(resultMeasureValue);
		result.setResultUnit(resultMeasureValue == null ? null : nwisResult.getParmUntTx());
		result.setResultValueStatus(RESULT_VALUE_STATUS.getOrDefault(nwisResult.getDqiCd(), null));
		result.setStatisticType(nwisResult.getParmStatTx() == null ? 
			(STATISTIC_TYPE.getOrDefault(remarkCd, null)) : nwisResult.getParmStatTx());
		result.setResultValueType(getResultValueType(nwisResult.getResultMd(), remarkCd));
		result.setWeightBasisType(nwisResult.getParmWtTx());
		result.setDurationBasis(nwisResult.getParmTmTx());
		result.setTemperatureBasisLevel(nwisResult.getParmTempTx());
		result.setParticleSize(nwisResult.getParmSizeTx());
		result.setPrecision(nwisResult.getLabStdVa());
		result.setResultComment(StringUtils.trimToNull(nwisResult.getResultLabCmTx()));
		result.setSampleTissueAnatomyName(nwisResult.getSampleTissueTaxonomicName());
		result.setSampleTissueAnatomyName(nwisResult.getSampleTissueAnatomyName());
		result.setAnalyticalProcedureId(nwisResult.getMethCd());
		result.setAnalyticalProcedureSource(nwisResult.getMethCd() == null ? null : "USGS");
		result.setAnalyticalMethodName(nwisResult.getMethNm());
		result.setAnalyticalMethodCitation(nwisResult.getCitNm());
		result.setLabName(nwisResult.getProtoOrgNm());
		result.setAnalysisStartDate(anlDt == null ? null :
					StringUtils.substring(anlDt, 0, 4) + "-" + StringUtils.substring(anlDt, 4, 6) + "-" 
					+ StringUtils.substring(anlDt, 6, 9));
		result.setLabRemark(getLabRemark(nwisResult.getValQualCd1Nm(),
				nwisResult.getValQualCd2Nm(),
				nwisResult.getValQualCd3Nm(),
				nwisResult.getValQualCd4Nm(),			
				nwisResult.getValQualCd5Nm(),
				remarkCd));
		result.setDetectionLimit(detectionLimit);
		result.setDetectionLimitUnit(detectionLimit == null ? null : nwisResult.getParmUntTx());
		result.setDetectionLimitDesc(detectionLimit == null ? null :
			getDetectionDesc(
					remarkCd, 
					nwisResult.getRptLevVa(), 
					nwisResult.getResultUnrndVa(), 
					nwisResult.getRptLevVa(),
					nwisResult.getRptLevNm()));
		result.setAnalysisPrepDateTx(prepDt == null ? null: 
				StringUtils.substring(prepDt, 0, 4) + "-" 
				+ StringUtils.substring(prepDt, 4, 6) + "-"
				+ StringUtils.substring(prepDt, 6, 8));
		
		return result;
	}
	
	private String getResultMeasureValue(String fxdTx, String remarkCd, String resultVa) {
		if (fxdTx != null) {
			return fxdTx;
		} else if (remarkCd.isEmpty()) {
			return resultVa;
		} else {
			switch (remarkCd) {
				case "U":
				case "M":
				case "N":
				case "<":
				case ">":
					return null;
				default:
					return resultVa;
			}
		}
	}
	
	private String getResultValueType(String resultMd, String remarkCd) {
		if (resultMd == null) {
			return "Calculated";
		} else if (remarkCd.contentEquals("E")) {
			return "Estimated";
		} else {
			return "Actual";
		}
	}
	
	private String getDetectionLimit(
			String remarkCd,
			String resultVa,
			String rptLevVa, 
			String resultUnrndVa, 
			String parmMethMultiplier, 
			String parmMultiplier, 
			String rptLevCd) {
		try {
			if (remarkCd.contentEquals("<") && rptLevVa == null) {
				return resultVa;
			} else if (remarkCd.contentEquals("<") && (Float.parseFloat(resultUnrndVa) > Float.parseFloat(rptLevVa))) {
				return resultVa;
			} else if (remarkCd.contentEquals(">")) {
				return resultVa;
			} else if ((remarkCd.contentEquals("N") || remarkCd.contentEquals("U")) && rptLevVa != null) {
				return rptLevVa;
			} else if (remarkCd.contentEquals("M") && rptLevVa == null) {
				if (resultUnrndVa == null) {
					return null;
				} else {
					return parmMethMultiplier == null ? parmMultiplier : parmMethMultiplier;
				}
			} else if (rptLevCd != null) {
				return rptLevVa;
			} else {
				return null;
			}
		} catch(NumberFormatException e) {
				return null;
		}
	}
	
	private String getDetectionDesc(
			String remarkCd,
			String rptLevVa, 
			String resultUnrndVa, 
			String rptLevCd,
			String wqxRptLevNm) {
		try {
			if (remarkCd.contentEquals("<") && rptLevVa == null) {
				return "Historical Lower Reporting Limit";
			} else if (remarkCd.contentEquals("<") && Float.parseFloat(resultUnrndVa) > Float.parseFloat(rptLevVa)) {
				return "Elevated Detection Limit";
			} else if (remarkCd.contentEquals("<") && rptLevCd != null) {
				return wqxRptLevNm;
			} else if (remarkCd.contentEquals(">")) {
				return "Upper Reporting Limit";
			} else if (remarkCd.contentEquals("M") && rptLevVa == null) {
				if (resultUnrndVa == null) {
					return null;
				} else {
					return "Lower Quantitation Limit";
				}
			} else if (remarkCd.contentEquals("M") && rptLevVa != null) {
				return wqxRptLevNm;
			} else if (rptLevVa != null) {
				return wqxRptLevNm;
			} else {
				return null;
			}
		} catch(NumberFormatException e) {
				return null;
		}
	}
	
	private String getLabRemark(String cd1Nm, String cd2Nm, String cd3Nm, String cd4Nm, String cd5Nm, String remarkCd) {
		return (cd1Nm == null ? "" : cd1Nm)
				+ (cd2Nm == null ? "" : cd2Nm)
				+ (cd3Nm == null ? "" : cd3Nm)
				+ (cd4Nm == null ? "" : cd4Nm)
				+ (cd5Nm == null ? "" : cd5Nm)
				+ (remarkCd.contentEquals("R") ? "Result below sample specific critical level." : "");
				
	}

}
