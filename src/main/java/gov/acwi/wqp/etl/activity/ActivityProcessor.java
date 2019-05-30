package gov.acwi.wqp.etl.activity;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.AbstractMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import gov.acwi.wqp.etl.BaseProcessor;
import gov.acwi.wqp.etl.ConfigurationService;
import gov.acwi.wqp.etl.nwis.activity.NwisActivity;

@Component
public class ActivityProcessor extends BaseProcessor implements ItemProcessor<NwisActivity, Activity> {

	private static final Map<String, String> ACTIVITY_TYPE_CODE_MAP = Map.ofEntries(
			new AbstractMap.SimpleEntry<>("A", "Not determined"),
			new AbstractMap.SimpleEntry<>("B", "Quality Control Sample-Other"),
			new AbstractMap.SimpleEntry<>("H", "Sample-Composite Without Parents"),
			new AbstractMap.SimpleEntry<>("1", "Quality Control Sample-Field Spike"),
			new AbstractMap.SimpleEntry<>("2", "Quality Control Sample-Field Blank"),
			new AbstractMap.SimpleEntry<>("3", "Quality Control Sample-Reference Sample"),
			new AbstractMap.SimpleEntry<>("4", "Quality Control Sample-Blind"),
			new AbstractMap.SimpleEntry<>("5", "Quality Control Sample-Field Replicate"),
			new AbstractMap.SimpleEntry<>("6", "Quality Control Sample-Reference Material"),
			new AbstractMap.SimpleEntry<>("7", "Quality Control Sample-Field Replicate"),
			new AbstractMap.SimpleEntry<>("8", "Quality Control Sample-Spike Solution"),
			new AbstractMap.SimpleEntry<>("9", "Sample-Routine"));
	private static final String DEFAULT_ACTIVITY_TYPE = "Unknown";
	private static final String FEET_UNIT = "feet";
	private static final String METERS_UNIT = "meters";
	private static final String NAWQA_PROJECT_ID = "National Water Quality Assessment Program (NAWQA)";
	private static final String BELOW_SEA_LEVEL_REF_POINT = "Below mean sea level";
	private static final String BELOW_LAND_SURFACE_REF_POINT = "Below land-surface datum";
	private static final String USGS = "USGS";
	private final ConfigurationService configurationService;

	@Autowired
	public ActivityProcessor(ConfigurationService configurationService) {
		this.configurationService = configurationService;
	}

	@Override
	public Activity process(NwisActivity nwisActivity) throws Exception {
		Activity activity = new Activity();

		String nwisSampleStartSg = nwisActivity.getSampleStartSg();
		boolean isSampleStartInMorH = isNonNullAndEqual(nwisSampleStartSg, "m")
				|| isNonNullAndEqual(nwisSampleStartSg, "h");
		LocalDateTime nwisSampleStartDt = nwisActivity.getSampleStartDt();

		String nwisSampleEndSg = nwisActivity.getSampleEndSg();
		boolean isSampleEndInMorH = isNonNullAndEqual(nwisSampleEndSg, "m") || isNonNullAndEqual(nwisSampleEndSg, "h");
		String nwisSampleEndDt = nwisActivity.getSampleEndDt();

		boolean isSampleCollectMethod = nwisActivity.getV84164FxdTx() != null && nwisActivity.getV82398FxdTx() != null;

		String nwisV00003 = nwisActivity.getV00003();
		String nwisV00098 = nwisActivity.getV00098();
		String nwisV78890 = nwisActivity.getV78890();
		String nwisV78891 = nwisActivity.getV78891();
		String nwisV72015 = nwisActivity.getV72015();
		String nwisV72016 = nwisActivity.getV72016();
		String nwisV82047 = nwisActivity.getV82047();
		String nwisV82048 = nwisActivity.getV82048();

		DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss");

		activity.setDataSourceId(configurationService.getEtlDataSourceId());
		activity.setDataSource(configurationService.getEtlDataSource());
		activity.setStationId(nwisActivity.getStationId());
		activity.setSiteId(nwisActivity.getSiteId());
		activity.setEventDate(nwisSampleStartDt.toLocalDate());
		activity.setActivity(
				nwisActivity.getNwisHost() + '.' + nwisActivity.getQwDbNo() + '.' + nwisActivity.getRecordNo());
		activity.setSampleMedia(nwisActivity.getWqxActMedNm());
		activity.setOrganization(nwisActivity.getOrganization());
		activity.setSiteType(nwisActivity.getSiteType());
		activity.setHuc(nwisActivity.getHuc());
		activity.setGovernmentalUnitCode(nwisActivity.getGovernmentalUnitCode());
		activity.setGeom(nwisActivity.getGeom());
		activity.setOrganizationName(nwisActivity.getOrganizationName());
		activity.setActivityId(nwisActivity.getSampleId());
		activity.setActivityTypeCode(
				ACTIVITY_TYPE_CODE_MAP.getOrDefault(nwisActivity.getSampTypeCd(), DEFAULT_ACTIVITY_TYPE));
		activity.setActivityMediaSubdivName(nwisActivity.getWqxActMedSub());
		activity.setActivityStartTime(isSampleStartInMorH ? timeFormatter.format(nwisSampleStartDt) : null);
		activity.setActStartTimeZone(isSampleStartInMorH ? nwisActivity.getSampleStartTimeDatumCd() : null);
		activity.setActivityStopDate(getActivityStopDate(nwisSampleEndSg, nwisSampleEndDt));
		activity.setActivityStopTime(isSampleEndInMorH ? StringUtils.substring(nwisSampleEndDt, 11) : null);
		activity.setActStopTimeZone(
				nwisSampleEndDt != null && isSampleEndInMorH ? nwisActivity.getSampleStartTimeDatumCd() : null);
		activity.setActivityDepth(getActivityDepth(nwisV00003, nwisV00098, nwisV78890, nwisV78891));
		activity.setActivityDepthUnit(getActivityDepthUnit(nwisV00003, nwisV00098, nwisV78890, nwisV78891));
		activity.setActivityDepthRefPoint(getActivityDepthRefPoint(nwisV00003, nwisV00098, nwisV78890, nwisV78891,
				nwisV72015, nwisV72016, nwisV82047));
		activity.setActivityUpperDepth(getActivityUpperDepth(nwisV72015, nwisV82047));
		activity.setActivityUpperDepthUnit(getActivityUpperDepthUnit(nwisV72015, nwisV82047));
		activity.setActivityLowerDepth(getActivityLowerDepth(nwisV72015, nwisV72016, nwisV82047, nwisV82048));
		activity.setActivityLowerDepthUnit(getActivityLowerDepthUnit(nwisV72015, nwisV72016, nwisV82047, nwisV82048));
		activity.setProjectId(getProjectId(nwisActivity.getNawqaSiteNo(), nwisActivity.getV50280(),
				nwisActivity.getV71999(), nwisSampleStartDt.toLocalDate()));
		activity.setActivityConductingOrg(
				getActivityConductingOrg(nwisActivity.getProtoOrgNm(), nwisActivity.getCollEntCd()));
		activity.setActivityComment(StringUtils.trimToNull(nwisActivity.getSampleLabCmTx()));
		activity.setSampleAqfrName(nwisActivity.getAqfrNm());
		activity.setHydrologicConditionName(nwisActivity.getHydCondNm());
		activity.setHydrologicEventName(nwisActivity.getHydEventNm());
		activity.setSampleCollectMethodId(isSampleCollectMethod ? nwisActivity.getV82398() : USGS);
		activity.setSampleCollectMethodCtx(isSampleCollectMethod ? USGS + " parameter code 82398" : USGS);
		activity.setSampleCollectMethodName(isSampleCollectMethod ? nwisActivity.getV82398FxdTx() : USGS);
		activity.setSampleCollectEquipName(isSampleCollectMethod ? nwisActivity.getV84164FxdTx() : "Unknown");

		return activity;
	}

	private String getActivityStopDate(String sampleEndSg, String sampleEndDt) {
		if (sampleEndSg == null) {
			return null;
		} else {
			switch (sampleEndSg) {
			case "m":
			case "h":
			case "D":
				return StringUtils.substring(sampleEndDt, 0, 10);
			case "M":
				return StringUtils.substring(sampleEndDt, 0, 7);
			case "Y":
				return StringUtils.substring(sampleEndDt, 0, 4);
			default:
				return null;
			}
		}
	}

	private String getActivityDepth(String v00003, String v00098, String v78890, String v78891) {
		if (v00003 != null) {
			return v00003;
		} else if (v00098 != null) {
			return v00098;
		} else if (v78890 != null) {
			return v78890;
		} else if (v78891 != null) {
			return v78891;
		} else {
			return null;
		}
	}

	private String getActivityDepthUnit(String v00003, String v00098, String v78890, String v78891) {
		if (v00003 != null) {
			return FEET_UNIT;
		} else if (v00098 != null) {
			return METERS_UNIT;
		} else if (v78890 != null) {
			return FEET_UNIT;
		} else if (v78891 != null) {
			return METERS_UNIT;
		} else {
			return null;
		}
	}

	private String getActivityDepthRefPoint(String v00003, String v00098, String v78890, String v78891, String v72015,
			String v72016, String v82047) {
		if (v00003 != null || v00098 != null) {
			return null;
		} else if (v78890 != null || v78891 != null) {
			return BELOW_SEA_LEVEL_REF_POINT;
		} else if (v72015 != null) {
			return BELOW_LAND_SURFACE_REF_POINT;
		} else if (v82047 != null) {
			return null;
		} else if (v72016 != null) {
			return BELOW_LAND_SURFACE_REF_POINT;
		} else {
			return null;
		}
	}

	private String getActivityUpperDepth(String v72015, String v82047) {
		if (v72015 != null) {
			return v72015;
		} else if (v82047 != null) {
			return v82047;
		} else {
			return null;
		}
	}

	private String getActivityUpperDepthUnit(String v72015, String v82047) {
		if (v72015 != null) {
			return FEET_UNIT;
		} else if (v82047 != null) {
			return METERS_UNIT;
		} else {
			return null;
		}
	}

	private String getActivityLowerDepth(String v72015, String v72016, String v82047, String v82048) {
		if (v72015 != null) {
			return v72016;
		} else if (v82047 != null) {
			return v82048;
		} else if (v72016 != null) {
			return v72016;
		} else if (v82048 != null) {
			return v82048;
		} else {
			return null;
		}
	}

	private String getActivityLowerDepthUnit(String v72015, String v72016, String v82047, String v82048) {
		if (v72015 != null && v72016 == null) {
			return null;
		} else if (v72015 != null && v72016 != null) {
			return FEET_UNIT;
		} else if (v82047 != null & v82048 == null) {
			return null;
		} else if (v82047 != null) {
			return METERS_UNIT;
		} else if (v72016 != null) {
			return FEET_UNIT;
		} else if (v82048 != null) {
			return METERS_UNIT;
		} else {
			return null;
		}
	}

	private String getProjectId(String nawqaSiteNo, String v50280, String v71999, LocalDate sampleStartDt) {
		String projectId = null;

		if (nawqaSiteNo != null) {
			if (sampleStartDt.compareTo(LocalDate.parse("2001-10-01")) >= 0) {
				if (isNonNullAndEqual(v71999, "20") || isNonNullAndEqual(v71999, "25")
						|| isNonNullAndEqual(v71999, "15")) {
					projectId = NAWQA_PROJECT_ID;
				} else if (v71999 == null && v50280 != null) {
					projectId = NAWQA_PROJECT_ID;
				}
			} else if (isNonNullAndEqual(v71999, "15") || v50280 != null) {
				projectId = NAWQA_PROJECT_ID;
			}
		}

		return projectId;
	}

	private String getActivityConductingOrg(String protoOrgNm, String collEntCd) {
		if (protoOrgNm != null) {
			return protoOrgNm;
		} else if (collEntCd != null) {
			return collEntCd;
		} else {
			return null;
		}
	}

}
