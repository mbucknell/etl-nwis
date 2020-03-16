package gov.acwi.wqp.etl.mysqlnwis.stat;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

public class StatRowMapper implements RowMapper<Stat> {

	public static final String STAT_CD_COLUMN_NAME = "stat_cd";
	public static final String DV_VALID_FG_COLUMN_NAME = "dv_valid_fg";
	public static final String UV_VALID_FG_COLUMN_NAME = "uv_valid_fg";
	public static final String STAT_NM_COLUMN_NAME = "stat_nm";
	public static final String STAT_DS_COLUMN_NAME = "stat_ds";

	@Override
	public Stat mapRow(ResultSet rs, int rowNum) throws SQLException {
		Stat stat = new Stat();
		stat.setStatCd(rs.getString(STAT_CD_COLUMN_NAME));
		stat.setDvValidFg(rs.getString(DV_VALID_FG_COLUMN_NAME).charAt(0) == 'Y');
		stat.setUvValidFg(rs.getString(UV_VALID_FG_COLUMN_NAME).charAt(0) == 'Y');
		stat.setStatNm(rs.getString(STAT_NM_COLUMN_NAME));
		stat.setStatDs(rs.getString(STAT_DS_COLUMN_NAME));

		return stat;
	}
}
