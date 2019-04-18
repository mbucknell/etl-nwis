package gov.acwi.wqp.etl.nwis;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.postgis.PGgeometry;
import org.springframework.jdbc.core.RowMapper;

public class NwisActivityRowMapper implements RowMapper<NwisActivity> {
	
	public static final String STATION_ID_COLUMN_NAME = "station_id";
	public static final String SITE_ID_COLUMN_NAME = "site_id";
	public static final String SAMPLE_START_DT_COLUMN_NAME = "sample_start_dt";
	public static final String NWIS_HOST_COLUMN_NAME = "nwis_host";
	public static final String QW_DB_NO_COLUMN_NAME = "qw_db_no";
	public static final String RECORD_NO_COLUMN_NAME = "record_no";
	public static final String WQX_ACT_MED_NM_COLUMN_NAME = "wqx_act_med_nm";
	public static final String ORGANIZATION_COLUMN_NAME = "organization";
	public static final String SITE_TYPE_COLUMN_NAME = "site_type";
	public static final String HUC_COLUMN_NAME = "huc";
	public static final String GOVERNMENTAL_UNIT_CODE_COLUMN_NAME = "governmental_unit_code";
	public static final String GEOM_COLUMN_NAME = "geom";
	public static final String ORGANIZATION_NAME_COLUMN_NAME = "organization_name";
	public static final String SAMPLE_ID_COLUMN_NAME = "sample_id";
	public static final String SAMP_TYPE_CD_COLUMN_NAME = "samp_type_cd";
	public static final String WQX_ACT_MED_SUB_COLUMN_NAME = "wqx_act_med_sub";
	public static final String SAMPLE_START_SG_COLUMN_NAME = "sample_start_sg";
	public static final String SAMPLE_START_TIME_DATUM_CD_COLUMN_NAME = "sample_start_time_datum_cd";
	public static final String SAMPLE_END_SG_COLUMN_NAME = "sample_end_sg";
	public static final String SAMPLE_END_DT_COLUMN_NAME = "sample_end_dt";
	public static final String V00003_COLUMN_NAME = "v00003";
	public static final String V00098_COLUMN_NAME = "v00098";
	public static final String V50280_COLUMN_NAME = "v50280";
	public static final String V71999_COLUMN_NAME = "v71999";
	public static final String V72015_COLUMN_NAME = "v72015";
	public static final String V72016_COLUMN_NAME = "v72016";
	public static final String V78890_COLUMN_NAME = "v78890";
	public static final String V78891_COLUMN_NAME = "v78891";
	public static final String V82047_COLUMN_NAME = "v82047";
	public static final String V82048_COLUMN_NAME = "v82048";
	public static final String V82398_COLUMN_NAME = "v82398";
	public static final String V82398_FXD_TX_COLUMN_NAME = "v82398_fxd_tx";
	public static final String V84164_FXD_TX_COLUMN_NAME = "v84164_fxd_tx";
	public static final String NAWQA_SITE_NO_COLUMN_NAME = "nawqa_site_no";
	public static final String PROTO_ORG_NM_COLUMN_NAME = "proto_org_nm";
	public static final String COLL_ENT_CD_COLUMN_NAME = "coll_ent_cd";
	public static final String SAMPLE_LAB_CM_TX_COLUMN_NAME = "sample_lab_cm_tx";
	public static final String AQFR_NM_COLUMN_NAME = "aqfr_nm";
	public static final String HYD_COND_NM_COLUMN_NAME = "hyd_cond_nm";
	public static final String HYD_EVENT_NM_COLUMN_NAME = "hyd_event_nm";
	
	private DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

	@Override
	public NwisActivity mapRow(ResultSet rs, int rowNum) throws SQLException {
		NwisActivity nwisActivity = new NwisActivity();
		
		nwisActivity.setStationId(rs.getInt(STATION_ID_COLUMN_NAME));
		nwisActivity.setSiteId(rs.getString(SITE_ID_COLUMN_NAME));
		nwisActivity.setSampleStartDt(LocalDateTime.parse(rs.getString(SAMPLE_START_DT_COLUMN_NAME), dateTimeFormatter));
		nwisActivity.setNwisHost(rs.getString(NWIS_HOST_COLUMN_NAME));
		nwisActivity.setQwDbNo(rs.getString(QW_DB_NO_COLUMN_NAME));
		nwisActivity.setRecordNo(rs.getString(RECORD_NO_COLUMN_NAME));
		nwisActivity.setWqxActMedNm(rs.getString(WQX_ACT_MED_NM_COLUMN_NAME));
		nwisActivity.setOrganization(rs.getString(ORGANIZATION_COLUMN_NAME));
		nwisActivity.setSiteType(rs.getString(SITE_TYPE_COLUMN_NAME));
		nwisActivity.setHuc(rs.getString(HUC_COLUMN_NAME));
		nwisActivity.setGovernmentalUnitCode(rs.getString(GOVERNMENTAL_UNIT_CODE_COLUMN_NAME));
		nwisActivity.setGeom((PGgeometry)rs.getObject(GEOM_COLUMN_NAME));
		nwisActivity.setOrganizationName(rs.getString(ORGANIZATION_NAME_COLUMN_NAME));
		nwisActivity.setSampleId(rs.getInt(SAMPLE_ID_COLUMN_NAME));
		nwisActivity.setSampTypeCd(rs.getString(SAMP_TYPE_CD_COLUMN_NAME));
		nwisActivity.setWqxActMedSub(rs.getString(WQX_ACT_MED_SUB_COLUMN_NAME));
		nwisActivity.setSampleStartSg(rs.getString(SAMPLE_START_SG_COLUMN_NAME));
		nwisActivity.setSampleStartTimeDatumCd(rs.getString(SAMPLE_START_TIME_DATUM_CD_COLUMN_NAME));
		nwisActivity.setSampleEndSg(rs.getString(SAMPLE_END_SG_COLUMN_NAME));
		nwisActivity.setSampleEndDt(rs.getString(SAMPLE_END_DT_COLUMN_NAME));
		nwisActivity.setV00003(rs.getString(V00003_COLUMN_NAME));
		nwisActivity.setV00098(rs.getString(V00098_COLUMN_NAME));
		nwisActivity.setV50280(rs.getString(V50280_COLUMN_NAME));
		nwisActivity.setV71999(rs.getString(V71999_COLUMN_NAME));
		nwisActivity.setV72015(rs.getString(V72015_COLUMN_NAME));
		nwisActivity.setV72016(rs.getString(V72016_COLUMN_NAME));
		nwisActivity.setV78890(rs.getString(V78890_COLUMN_NAME));
		nwisActivity.setV78891(rs.getString(V78891_COLUMN_NAME));
		nwisActivity.setV82047(rs.getString(V82047_COLUMN_NAME));
		nwisActivity.setV82048(rs.getString(V82048_COLUMN_NAME));
		nwisActivity.setV82398(rs.getString(V82398_COLUMN_NAME));
		nwisActivity.setV82398FxdTx(rs.getString(V82398_FXD_TX_COLUMN_NAME));
		nwisActivity.setV84164FxdTx(rs.getString(V84164_FXD_TX_COLUMN_NAME));
		nwisActivity.setNawqaSiteNo(rs.getString(NAWQA_SITE_NO_COLUMN_NAME));
		nwisActivity.setProtoOrgNm(rs.getString(PROTO_ORG_NM_COLUMN_NAME));
		nwisActivity.setCollEntCd(rs.getString(COLL_ENT_CD_COLUMN_NAME));
		nwisActivity.setSampleLabCmTx(rs.getString(SAMPLE_LAB_CM_TX_COLUMN_NAME));
		nwisActivity.setAqfrNm(rs.getString(AQFR_NM_COLUMN_NAME));
		nwisActivity.setHydCondNm(rs.getString(HYD_COND_NM_COLUMN_NAME));
		nwisActivity.setHydEventNm(rs.getString(HYD_EVENT_NM_COLUMN_NAME));
		
		return nwisActivity;
	}
}
