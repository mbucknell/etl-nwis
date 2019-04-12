package gov.acwi.wqp.etl.result;

import org.springframework.batch.item.ItemProcessor;

import gov.acwi.wqp.etl.Application;
import gov.acwi.wqp.etl.nwis.NwisResult;

public class ResultProcessor implements ItemProcessor<NwisResult, Result>{

	@Override
	public Result process(NwisResult nwisResult) throws Exception {
		Result result = new Result();
		
		result.setDataSourceId(Application.DATA_SOURCE_ID);
		result.setDataSource(Application.DATA_SOURCE);
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
		result.setActivityConductionOrg(nwisResult.getActivityConductionOrg());
		result.setActivityComment(nwisResult.getActivityComment());
		result.setSampleAqfrName(nwisResult.getSampleAqfrName());
		result.setHydrologicConditionName(nwisResult.getHydrologicConditionName());
		result.setHydrologicEventName(nwisResult.getHydrologicEventName());
		result.setSampleCollectMethodId(nwisResult.getSampleCollectMethodId());
		result.setSampleCollectMethodCtx(nwisResult.getSampleCollectMethodCtx());
		result.setSampleCollectMethodName(nwisResult.getSampleCollectMethodName());
		result.setSampleCollectEquipName(nwisResult.getSampleCollectEquipName());
		
		return result;
	}

}
