package gov.acwi.wqp.etl.nwis;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;

import org.springframework.jdbc.core.RowMapper;

public class NwisDistrictCdsByHostRowMapper implements RowMapper<NwisDistrictCdsByHost> {
	
	public static final String HOST_NAME_COLUMN_NAME = "host_name";
	public static final String DISTRICT_CD_COLUMN_NAME = "district_cd";
	public static final String STATE_NAME_COLUMN_NAME = "state_name";
	public static final String STATE_POSTAL_CD_COLUMN_NAME = "state_postal_cd";
	public static final String LAST_UPD_DATE_COLUMN_NAME = "last_upd_date";
	
	@Override
	public NwisDistrictCdsByHost mapRow(ResultSet rs, int rowNum) throws SQLException {
		NwisDistrictCdsByHost nwisDistictCds = new NwisDistrictCdsByHost();
		nwisDistictCds.setHostName(rs.getString(HOST_NAME_COLUMN_NAME));
		nwisDistictCds.setDistrictCd(rs.getString(DISTRICT_CD_COLUMN_NAME));
		nwisDistictCds.setStateName(rs.getString(STATE_NAME_COLUMN_NAME));
		nwisDistictCds.setStatePostalCd(rs.getString(STATE_POSTAL_CD_COLUMN_NAME));
		nwisDistictCds.setLastUpdDate(LocalDate.parse(rs.getString(LAST_UPD_DATE_COLUMN_NAME)));
		
		return nwisDistictCds;
	}

}
