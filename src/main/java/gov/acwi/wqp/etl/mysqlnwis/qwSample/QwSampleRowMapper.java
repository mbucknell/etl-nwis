package gov.acwi.wqp.etl.mysqlnwis.qwSample;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.springframework.jdbc.core.RowMapper;

public class QwSampleRowMapper implements RowMapper<QwSample> {
	
	public static final String SAMPLE_ID_COLUMN_NAME = "sample_id";
	public static final String SITE_ID_COLUMN_NAME = "site_id";
	public static final String RECORD_NO_COLUMN_NAME = "record_no";
	public static final String NWIS_HOST_COLUMN_NAME = "nwis_host";
	public static final String DB_NO_COLUMN_NAME = "db_no";
	public static final String QW_DB_NO_COLUMN_NAME = "qw_db_no";
	public static final String SAMPLE_WEB_CD_COLUMN_NAME = "sample_web_cd";
	public static final String SAMPLE_START_DT_COLUMN_NAME = "sample_start_dt";
	public static final String SAMPLE_START_DISPLAY_DT_COLUMN_NAME = "sample_start_display_dt";
	public static final String SAMPLE_START_SG_COLUMN_NAME = "sample_start_sg";
	public static final String SAMPLE_END_DT_COLUMN_NAME = "sample_end_dt";
	public static final String SAMPLE_END_DISPLAY_DT_COLUMN_NAME = "sample_end_display_dt";
	public static final String SAMPLE_END_SG_COLUMN_NAME = "sample_end_sg";
	public static final String SAMPLE_UTC_START_DT_COLUMN_NAME = "sample_utc_start_dt";
	public static final String SAMPLE_UTC_START_DISPLAY_DT_COLUMN_NAME = "sample_utc_start_display_dt";
	public static final String SAMPLE_UTC_END_DT_COLUMN_NAME = "sample_utc_end_dt";
	public static final String SAMPLE_UTC_END_DISPLAY_DT_COLUMN_NAME = "sample_utc_end_display_dt";
	public static final String SAMPLE_START_TIME_DATUM_CD_COLUMN_NAME = "sample_start_time_datum_cd";
	public static final String MEDIUM_CD_COLUMN_NAME = "medium_cd";
	public static final String TU_ID_COLUMN_NAME = "tu_id";
	public static final String BODY_PART_ID_COLUMN_NAME = "body_part_id";
	public static final String NWIS_SAMPLE_ID_COLUMN_NAME = "nwis_sample_id";
	public static final String LAB_NO_COLUMN_NAME = "lab_no";
	public static final String PROJECT_CD_COLUMN_NAME = "project_cd";
	public static final String AQFR_CD_COLUMN_NAME = "aqfr_cd";
	public static final String SAMP_TYPE_CD_COLUMN_NAME = "samp_type_cd";
	public static final String SAMPLE_LAB_CM_TX_COLUMN_NAME = "sample_lab_cm_tx";
	public static final String SAMPLE_FIELD_CM_TX_COLUMN_NAME = "sample_field_cm_tx";
	public static final String COLL_ENT_CD_COLUMN_NAME = "coll_ent_cd";
	public static final String ANL_STAT_CD_COLUMN_NAME = "anl_stat_cd";
	public static final String ANL_SRC_CD_COLUMN_NAME = "anl_src_cd";
	public static final String ANL_TYPE_TX_COLUMN_NAME = "anl_type_tx";
	public static final String HYD_COND_CD_COLUMN_NAME = "hyd_cond_cd";
	public static final String HYD_EVENT_CD_COLUMN_NAME = "hyd_event_cd";
	public static final String TM_DATUM_RLBTY_CD_COLUMN_NAME = "tm_datum_rlbty_cd";
	public static final String SAMPLE_MD_COLUMN_NAME = "sample_md";
	public static final String QW_SAMPLE_MD_COLUMN_NAME = "qw_sample_md";

	private DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
	
	@Override
	public QwSample mapRow(ResultSet rs, int rowNum) throws SQLException {
		QwSample qwSample = new QwSample();
		qwSample.setSampleId(rs.getInt(SAMPLE_ID_COLUMN_NAME));
		qwSample.setSiteId(rs.getInt(SITE_ID_COLUMN_NAME));
		qwSample.setRecordNo(rs.getString(RECORD_NO_COLUMN_NAME));
		qwSample.setNwisHost(rs.getString(NWIS_HOST_COLUMN_NAME));
		qwSample.setDbNo(rs.getString(DB_NO_COLUMN_NAME));
		qwSample.setQwDbNo(rs.getString(QW_DB_NO_COLUMN_NAME));
		qwSample.setSampleWebCd(rs.getString(SAMPLE_WEB_CD_COLUMN_NAME).charAt(0) =='Y');
		qwSample.setSampleStartDt(LocalDateTime.parse(rs.getString(SAMPLE_START_DT_COLUMN_NAME), dateTimeFormatter));
		qwSample.setSampleStartDisplayDt(rs.getString(SAMPLE_START_DISPLAY_DT_COLUMN_NAME));
		qwSample.setSampleStartSg(rs.getString(SAMPLE_START_SG_COLUMN_NAME));
		qwSample.setSampleEndDt(rs.getString(SAMPLE_END_DT_COLUMN_NAME));
		qwSample.setSampleEndDisplayDt(rs.getString(SAMPLE_END_DISPLAY_DT_COLUMN_NAME));
		qwSample.setSampleEndSg(rs.getString(SAMPLE_END_SG_COLUMN_NAME));
		qwSample.setSampleUtcStartDt(LocalDateTime.parse(rs.getString(SAMPLE_UTC_START_DT_COLUMN_NAME), dateTimeFormatter));
		qwSample.setSampleUtcStartDisplayDt(rs.getString(SAMPLE_UTC_START_DISPLAY_DT_COLUMN_NAME));
		qwSample.setSampleUtcEndDt(rs.getString(SAMPLE_UTC_END_DT_COLUMN_NAME));
		qwSample.setSampleUtcEndDisplayDt(rs.getString(SAMPLE_UTC_END_DISPLAY_DT_COLUMN_NAME));
		qwSample.setSampleStartTimeDatumCd(rs.getString(SAMPLE_START_TIME_DATUM_CD_COLUMN_NAME));
		qwSample.setMediumCd(rs.getString(MEDIUM_CD_COLUMN_NAME));
		qwSample.setTuId(rs.getString(TU_ID_COLUMN_NAME));
		qwSample.setBodyPartId(rs.getString(BODY_PART_ID_COLUMN_NAME));
		qwSample.setNwisSampleId(rs.getString(NWIS_SAMPLE_ID_COLUMN_NAME));
		qwSample.setLabNo(rs.getString(LAB_NO_COLUMN_NAME));
		qwSample.setProjectCd(rs.getString(PROJECT_CD_COLUMN_NAME));
		qwSample.setAqfrCd(rs.getString(AQFR_CD_COLUMN_NAME));
		qwSample.setSampTypeCd(rs.getString(SAMP_TYPE_CD_COLUMN_NAME));
		qwSample.setSampleLabCmTx(rs.getString(SAMPLE_LAB_CM_TX_COLUMN_NAME));
		qwSample.setSampleFieldCmTx(rs.getString(SAMPLE_FIELD_CM_TX_COLUMN_NAME));
		qwSample.setCollEntCd(rs.getString(COLL_ENT_CD_COLUMN_NAME));
		qwSample.setAnlStatCd(rs.getString(ANL_STAT_CD_COLUMN_NAME));
		qwSample.setAnlSrcCd(rs.getString(ANL_SRC_CD_COLUMN_NAME));
		qwSample.setAnlTypeTx(rs.getString(ANL_TYPE_TX_COLUMN_NAME));
		qwSample.setHydCondCd(rs.getString(HYD_COND_CD_COLUMN_NAME));
		qwSample.setHydEventCd(rs.getString(HYD_EVENT_CD_COLUMN_NAME));
		qwSample.setTmDatumRlbtyCd(rs.getString(TM_DATUM_RLBTY_CD_COLUMN_NAME));
		qwSample.setSampleMd(LocalDate.parse(rs.getString(SAMPLE_MD_COLUMN_NAME), dateTimeFormatter));
		qwSample.setQwSampleMd(LocalDate.parse(rs.getString(QW_SAMPLE_MD_COLUMN_NAME), dateTimeFormatter));
		
		return qwSample;
	}
}
