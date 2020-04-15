package gov.acwi.wqp.etl.mysqlnwis.gwlevels;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.springframework.jdbc.core.RowMapper;

public class GwLevelsRowMapper implements RowMapper<GwLevels> {
	private DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

	public static final String SITE_ID_COLUMN_NAME = "site_id";
	public static final String LEV_STR_DT = "lev_str_dt";
	public static final String LEV_DTM = "lev_dtm";
	public static final String LEV_DT_ACY_CD = "lev_dt_acy_cd";
	public static final String LEV_TZ_CD = "lev_tz_cd";
	public static final String LEV_TZ_OFFSET = "lev_tz_offset";
	public static final String LEV_VA = "lev_va";
	public static final String LEV_ACY_CD = "lev_acy_cd";
	public static final String PARAMETER_CD = "parameter_cd";
	public static final String LEV_DATUM_CD = "lev_datum_cd";
	public static final String LEV_SRC_CD = "lev_src_cd";
	public static final String LEV_STATUS_CD = "lev_status_cd";
	public static final String LEV_METH_CD = "lev_meth_cd";
	public static final String LEV_AGENCY_CD = "lev_agency_cd";
	public static final String LEV_AGE_CD = "lev_age_cd";
	public static final String GW_LEVELS_MD = "gw_levels_md";

	public GwLevels mapRow(ResultSet rs, int rowNum) throws SQLException {
		GwLevels qwResult = new GwLevels();

		qwResult.setSiteId(rs.getInt(SITE_ID_COLUMN_NAME));
		qwResult.setLevStrDt(rs.getString(LEV_STR_DT));
		qwResult.setLevDtm(LocalDateTime.parse(rs.getString(LEV_DTM), dateTimeFormatter));
		qwResult.setLevDtAcyCd(rs.getString(LEV_DT_ACY_CD));
		qwResult.setLevTzCd(rs.getString(LEV_TZ_CD));
		qwResult.setLevTzOffset(rs.getString(LEV_TZ_OFFSET));
		qwResult.setLevVa(rs.getString(LEV_VA));
		qwResult.setLevAcyCd(rs.getString(LEV_ACY_CD));
		qwResult.setParameterCd(rs.getString(PARAMETER_CD));
		qwResult.setLevDatumCd(rs.getString(LEV_DATUM_CD));
		qwResult.setLevSrcCd(rs.getString(LEV_SRC_CD));
		qwResult.setLevStatusCd(rs.getString(LEV_STATUS_CD));
		qwResult.setLevMethCd(rs.getString(LEV_METH_CD));
		qwResult.setLevAgencyCd(rs.getString(LEV_AGENCY_CD));
		qwResult.setLevAgeCd(rs.getString(LEV_AGE_CD));
		qwResult.setGwLevelsMd(LocalDateTime.parse(rs.getString(GW_LEVELS_MD), dateTimeFormatter));

		return qwResult;
	}
}
