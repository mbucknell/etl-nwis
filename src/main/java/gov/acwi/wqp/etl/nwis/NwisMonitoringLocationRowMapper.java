package gov.acwi.wqp.etl.nwis;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.postgis.PGgeometry;
import org.springframework.jdbc.core.RowMapper;

public class NwisMonitoringLocationRowMapper implements RowMapper<NwisMonitoringLocation> {
	
	public static final String SITE_ID_COLUMN_NAME = "";
	public static final String AGENCY_CD_COLUMN_NAME = "";
	public static final String SITE_NO_COLUMN_NAME = "";
	public static final String ORGANIZATION_ID_COLUMN_NAME = "";
	public static final String PRIMARY_SITE_TYPE_COLUMN_NAME = "";
	public static final String CALCULCATED_HUC_12_COLUMN_NAME = "";
	public static final String HUC_CD_COLUMN_NAME = "";
	public static final String COUNTRY_CD_COLUMN_NAME = "";
	public static final String STATE_CD_COLUMN_NAME = "";
	public static final String COUNTY_CD_COLUMN_NAME = "";
	public static final String GEOM_COLUMN_NAME = "";
	public static final String STATION_NM_COLUMN_NAME = "";
	public static final String ORGANIZATION_NAME_COLUMN_NAME = "";
	public static final String STATION_TYPE_NAME_COLUMN_NAME = "";
	public static final String DEC_LAT_VA_COLUMN_NAME = "";
	public static final String DEC_LONG_VA_COLUMN_NAME = "";
	public static final String MAP_SCALE_FC_COLUMN_NAME = "";
	public static final String LAT_LONG_METHOD_DESCRIPTION_COLUMN_NAME = "";
	public static final String DEC_COORD_DATUM_CD_COLUMN_NAME = "";
	public static final String ALT_DATUM_CD_COLUMN_NAME = "";
	public static final String ALT_VA_COLUMN_NAME = "";
	public static final String ALTITUDE_METHOD_DESCRIPTION_COLUMN_NAME = "";
	public static final String DRAIN_AREA_VA_COLUMN_NAME = "";
	public static final String CONTRIB_DRAIN_AREA_VA_COLUMN_NAME = "";
	public static final String LAT_LONG_ACCURACY_COLUMN_NAME = "";
	public static final String LAT_LONG_ACUURACY_UNIT_COLUMN_NAME = "";
	public static final String NAT_AQFR_NAME_COLUMN_NAME = "";
	public static final String AQFR_NM_COLUMN_NAME = "";
	public static final String AQUIFER_TYPE_DESCRIPTION_COLUMN_NAME = "";
	public static final String CONSTRUCTIVE_DATE_COLUMN_NAME = "";
	public static final String WELL_DEPTH_VA_COLUMN_NAME = "";
	public static final String HOLE_DEPTH_VA_COLUMN_NAME = "";

	@Override
	public NwisMonitoringLocation mapRow(ResultSet rs, int rowNum) throws SQLException {
		NwisMonitoringLocation nwisML = new NwisMonitoringLocation();
		
		nwisML.setSiteId(rs.getInt(SITE_ID_COLUMN_NAME));
		nwisML.setAgencyCd(rs.getString(AGENCY_CD_COLUMN_NAME));
		nwisML.setSiteNo(rs.getString(SITE_NO_COLUMN_NAME));
		nwisML.setOrganizationId(rs.getString(ORGANIZATION_ID_COLUMN_NAME));
		nwisML.setPrimarySiteType(rs.getString(PRIMARY_SITE_TYPE_COLUMN_NAME));
		nwisML.setCalculatedHuc12(rs.getString(CALCULCATED_HUC_12_COLUMN_NAME));
		nwisML.setHucCd(rs.getString(HUC_CD_COLUMN_NAME));
		nwisML.setCountryCd(rs.getString(COUNTRY_CD_COLUMN_NAME));
		nwisML.setStateCd(rs.getString(STATE_CD_COLUMN_NAME));
		nwisML.setCountyCd(rs.getString(COUNTY_CD_COLUMN_NAME));
		nwisML.setGeom((PGgeometry)rs.getObject(GEOM_COLUMN_NAME));
		nwisML.setStationNm(rs.getString(STATION_NM_COLUMN_NAME));
		nwisML.setOrganizationName(rs.getString(ORGANIZATION_NAME_COLUMN_NAME));
		nwisML.setStationTypeName(rs.getString(STATION_TYPE_NAME_COLUMN_NAME));
		nwisML.setDecLatVa(rs.getBigDecimal(DEC_LAT_VA_COLUMN_NAME));
		nwisML.setDecLongVa(rs.getBigDecimal(DEC_LONG_VA_COLUMN_NAME));
		nwisML.setMapScaleFc(rs.getString(MAP_SCALE_FC_COLUMN_NAME));
		nwisML.setLatLongMethodDescription(rs.getString(LAT_LONG_METHOD_DESCRIPTION_COLUMN_NAME));
		nwisML.setDecCoordDatumCd(rs.getString(DEC_COORD_DATUM_CD_COLUMN_NAME));
		nwisML.setAltDatumCd(rs.getString(ALT_DATUM_CD_COLUMN_NAME));
		nwisML.setAltVa(rs.getString(ALT_VA_COLUMN_NAME));
		nwisML.setAltitudeMethodDescription(rs.getString(ALTITUDE_METHOD_DESCRIPTION_COLUMN_NAME));
		nwisML.setDrainAreaVa(rs.getString(DRAIN_AREA_VA_COLUMN_NAME));
		nwisML.setContribDrainAreaVa(rs.getString(CONTRIB_DRAIN_AREA_VA_COLUMN_NAME));
		nwisML.setLatLongAccuracy(rs.getString(LAT_LONG_ACCURACY_COLUMN_NAME));
		nwisML.setLatLongAccuracyUnit(rs.getString(LAT_LONG_ACUURACY_UNIT_COLUMN_NAME));
		nwisML.setNatAqfrName(rs.getString(NAT_AQFR_NAME_COLUMN_NAME));
		nwisML.setAqfrNm(rs.getString(AQFR_NM_COLUMN_NAME));
		nwisML.setAquiferTypeDescription(rs.getString(AQUIFER_TYPE_DESCRIPTION_COLUMN_NAME));
		nwisML.setConstructionDate(rs.getString(CONSTRUCTIVE_DATE_COLUMN_NAME));
		nwisML.setWellDepthVa(rs.getString(WELL_DEPTH_VA_COLUMN_NAME));
		nwisML.setHoleDepthVa(rs.getString(HOLE_DEPTH_VA_COLUMN_NAME));
		
		return nwisML;
	}
}
