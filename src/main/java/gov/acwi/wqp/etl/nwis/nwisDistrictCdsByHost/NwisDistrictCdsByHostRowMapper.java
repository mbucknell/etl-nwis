package gov.acwi.wqp.etl.nwis.nwisDistrictCdsByHost;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;

import org.springframework.jdbc.core.RowMapper;

public class NwisDistrictCdsByHostRowMapper implements RowMapper<NwisDistrictCdsByHost> {
	
	private static final String DISTRICT_CD_COLUMN_NAME = "district_cd";
	private static final String STATE_NAME_COLUMN_NAME = "state_name";
	private static final String STATE_POSTAL_CD_COLUMN_NAME = "state_postal_cd";

	@Override
	public NwisDistrictCdsByHost mapRow(ResultSet rs, int rowNum) throws SQLException {
		NwisDistrictCdsByHost nwisDistictCds = new NwisDistrictCdsByHost();
		nwisDistictCds.setDistrictCd(rs.getString(DISTRICT_CD_COLUMN_NAME));
		nwisDistictCds.setStateName(rs.getString(STATE_NAME_COLUMN_NAME));
		nwisDistictCds.setStatePostalCd(rs.getString(STATE_POSTAL_CD_COLUMN_NAME));

		return nwisDistictCds;
	}

}
