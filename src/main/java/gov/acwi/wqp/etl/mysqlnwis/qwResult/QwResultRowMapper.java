package gov.acwi.wqp.etl.mysqlnwis.qwResult;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

public class QwResultRowMapper implements RowMapper<QwResult> {
	public static final String SAMPLE_ID_COLUMN_NAME = "sample_id";
	public static final String SITE_ID_COLUMN_NAME = "site_id";
	public static final String RECORD_NO_COLUMN_NAME = "record_no";
	public static final String RESULT_WEB_CD_COLUMN_NAME = "result_web_cd";
	public static final String PARAMETER_CD_COLUMN_NAME = "parameter_cd";
	public static final String METH_CD_COLUMN_NAME = "meth_cd";
	public static final String RESULT_VA_COLUMN_NAME = "result_va";
    public static final String RESULT_UNRND_VA_COLUMN_NAME = "result_unrnd_va";
    public static final String RESULT_RD_COLUMN_NAME = "result_rd";
    public static final String RPT_LEV_VA_COLUMN_NAME = "rpt_lev_va";
    public static final String RPT_LEV_CD_COLUMN_NAME = "rpt_lev_cd";
    public static final String LAB_STD_VA_COLUMN_NAME = "lab_std_va";
    public static final String REMARK_CD_COLUMN_NAME = "remark_cd";
    public static final String VAL_QUAL_TX_COLUMN_NAME = "val_qual_tx";
    public static final String NULL_VAL_QUAL_CD_COLUMN_NAME = "null_val_qual_cd";
    public static final String QA_CD_COLUMN_NAME = "qa_cd";
    public static final String DQI_CD_COLUMN_NAME = "dqi_cd";
    public static final String ANL_ENT_CD_COLUMN_NAME = "anl_ent_cd";
    public static final String ANL_SET_NO_COLUMN_NAME = "anl_set_no";
    public static final String ANL_DT_COLUMN_NAME = "anl_dt";
    public static final String PREP_SET_NO_COLUMN_NAME = "prep_set_no";
    public static final String PREP_DT_COLUMN_NAME = "prep_dt";
    public static final String RESULT_FIELD_CM_TX_COLUMN_NAME = "result_field_cm_tx";
    public static final String RESULT_LAB_CM_TX_COLUMN_NAME = "result_lab_cm_tx";
    public static final String RESULT_MD_COLUMN_NAME = "result_md";
    public static final String QW_RESULT_MD_COLUMN_NAME = "qw_result_md";
    
    public QwResult mapRow(ResultSet rs, int rowNum) throws SQLException {
    	QwResult qwResult = new QwResult();
    	
    	qwResult.setSampleId(rs.getInt(SAMPLE_ID_COLUMN_NAME));
    	qwResult.setSiteId(rs.getInt(SITE_ID_COLUMN_NAME));
    	qwResult.setRecordNo(rs.getString(RECORD_NO_COLUMN_NAME));
    	qwResult.setResultWebCd(rs.getString(RESULT_WEB_CD_COLUMN_NAME).charAt(0) == 'Y');
    	qwResult.setParameterCd(rs.getString(PARAMETER_CD_COLUMN_NAME));
    	qwResult.setMethCd(rs.getString(METH_CD_COLUMN_NAME));
    	qwResult.setResultVa(rs.getString(RESULT_VA_COLUMN_NAME));
    	qwResult.setResultUnrndVa(rs.getString(RESULT_UNRND_VA_COLUMN_NAME));
    	qwResult.setResultRd(rs.getString(RESULT_RD_COLUMN_NAME));
    	qwResult.setRptLevVa(rs.getString(RPT_LEV_VA_COLUMN_NAME));
    	qwResult.setRptLevCd(rs.getString(RPT_LEV_CD_COLUMN_NAME));
    	qwResult.setLabStdVa(rs.getString(LAB_STD_VA_COLUMN_NAME));
    	qwResult.setRemarkCd(rs.getString(REMARK_CD_COLUMN_NAME));
    	qwResult.setValQualTx(rs.getString(VAL_QUAL_TX_COLUMN_NAME));
    	qwResult.setNullValQualCd(rs.getString(NULL_VAL_QUAL_CD_COLUMN_NAME));
    	qwResult.setQaCd(rs.getString(QA_CD_COLUMN_NAME));
    	qwResult.setDqiCd(rs.getString(DQI_CD_COLUMN_NAME));
    	qwResult.setAnlEntCd(rs.getString(ANL_ENT_CD_COLUMN_NAME));
    	qwResult.setAnlSetNo(rs.getString(ANL_SET_NO_COLUMN_NAME));
    	qwResult.setAnlDt(rs.getString(ANL_DT_COLUMN_NAME));
    	qwResult.setPrepSetNo(rs.getString(PREP_SET_NO_COLUMN_NAME));
    	qwResult.setPrepDt(rs.getString(PREP_DT_COLUMN_NAME));
    	qwResult.setResultFieldCmTx(rs.getString(RESULT_FIELD_CM_TX_COLUMN_NAME));
    	qwResult.setResultLabCmTx(rs.getString(RESULT_LAB_CM_TX_COLUMN_NAME));
    	qwResult.setResultMd(rs.getDate(RESULT_MD_COLUMN_NAME));
    	qwResult.setQwResultMd(rs.getDate(QW_RESULT_MD_COLUMN_NAME));
    	
    	
    	return qwResult;
    }
}
