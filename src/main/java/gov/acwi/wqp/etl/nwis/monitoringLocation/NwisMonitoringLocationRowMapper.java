package gov.acwi.wqp.etl.nwis.monitoringLocation;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.postgis.PGgeometry;
import org.springframework.jdbc.core.RowMapper;

public class NwisMonitoringLocationRowMapper implements RowMapper<NwisMonitoringLocation> {
	
	private static final String SITE_ID_COLUMN_NAME = "site_id";
	private static final String AGENCY_CD_COLUMN_NAME = "agency_cd";
	private static final String SITE_NO_COLUMN_NAME = "site_no";
	private static final String ORGANIZATION_ID_COLUMN_NAME = "organization_id";
	private static final String PRIMARY_SITE_TYPE_COLUMN_NAME = "primary_site_type";
	private static final String CALCULATED_HUC_12_COLUMN_NAME = "calculated_huc_12";
	private static final String HUC_CD_COLUMN_NAME = "huc_cd";
	private static final String COUNTRY_CD_COLUMN_NAME = "country_cd";
	private static final String STATE_CD_COLUMN_NAME = "state_cd";
	private static final String COUNTY_CD_COLUMN_NAME = "county_cd";
	private static final String GEOM_COLUMN_NAME = "geom";
	private static final String STATION_NM_COLUMN_NAME = "station_nm";
	private static final String ORGANIZATION_NAME_COLUMN_NAME = "organization_name";
	private static final String STATION_TYPE_NAME_COLUMN_NAME = "station_type_name";
	private static final String DEC_LAT_VA_COLUMN_NAME = "dec_lat_va";
	private static final String DEC_LONG_VA_COLUMN_NAME = "dec_long_va";
	private static final String MAP_SCALE_FC_COLUMN_NAME = "map_scale_fc";
	private static final String LAT_LONG_METHOD_DESCRIPTION_COLUMN_NAME = "lat_long_method_description";
	private static final String DEC_COORD_DATUM_CD_COLUMN_NAME = "dec_coord_datum_cd";
	private static final String ALT_DATUM_CD_COLUMN_NAME = "alt_datum_cd";
	private static final String ALT_VA_COLUMN_NAME = "alt_va";
	private static final String ALT_ACY_VA_COLUMN_NAME = "alt_acy_va";
	private static final String ALTITUDE_METHOD_DESCRIPTION_COLUMN_NAME = "altitude_method_description";
	private static final String DRAIN_AREA_VA_COLUMN_NAME = "drain_area_va";
	private static final String CONTRIB_DRAIN_AREA_VA_COLUMN_NAME = "contrib_drain_area_va";
	private static final String LAT_LONG_ACCURACY_COLUMN_NAME = "lat_long_accuracy";
	private static final String LAT_LONG_ACCURACY_UNIT_COLUMN_NAME = "lat_long_accuracy_unit";
	private static final String NAT_AQFR_NAME_COLUMN_NAME = "nat_aqfr_name";
	private static final String AQFR_NM_COLUMN_NAME = "aqfr_nm";
	private static final String AQUIFER_TYPE_DESCRIPTION_COLUMN_NAME = "aquifer_type_description";
	private static final String CONSTRUCTION_DT_COLUMN_NAME = "construction_dt";
	private static final String WELL_DEPTH_VA_COLUMN_NAME = "well_depth_va";
	private static final String HOLE_DEPTH_VA_COLUMN_NAME = "hole_depth_va";

	@Override
	public NwisMonitoringLocation mapRow(ResultSet rs, int rowNum) throws SQLException {
		NwisMonitoringLocation nwisML = new NwisMonitoringLocation();
		
		nwisML.setSiteId(rs.getInt(SITE_ID_COLUMN_NAME));
		nwisML.setAgencyCd(rs.getString(AGENCY_CD_COLUMN_NAME));
		nwisML.setSiteNo(rs.getString(SITE_NO_COLUMN_NAME));
		nwisML.setOrganizationId(rs.getString(ORGANIZATION_ID_COLUMN_NAME));
		nwisML.setPrimarySiteType(rs.getString(PRIMARY_SITE_TYPE_COLUMN_NAME));
		nwisML.setCalculatedHuc12(rs.getString(CALCULATED_HUC_12_COLUMN_NAME));
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
		nwisML.setAltAcyVa(rs.getString(ALT_ACY_VA_COLUMN_NAME));
		nwisML.setAltitudeMethodDescription(rs.getString(ALTITUDE_METHOD_DESCRIPTION_COLUMN_NAME));
		nwisML.setDrainAreaVa(rs.getString(DRAIN_AREA_VA_COLUMN_NAME));
		nwisML.setContribDrainAreaVa(rs.getString(CONTRIB_DRAIN_AREA_VA_COLUMN_NAME));
		nwisML.setLatLongAccuracy(rs.getString(LAT_LONG_ACCURACY_COLUMN_NAME));
		nwisML.setLatLongAccuracyUnit(rs.getString(LAT_LONG_ACCURACY_UNIT_COLUMN_NAME));
		nwisML.setNatAqfrName(rs.getString(NAT_AQFR_NAME_COLUMN_NAME));
		nwisML.setAqfrNm(rs.getString(AQFR_NM_COLUMN_NAME));
		nwisML.setAquiferTypeDescription(rs.getString(AQUIFER_TYPE_DESCRIPTION_COLUMN_NAME));
		nwisML.setConstructionDate(rs.getString(CONSTRUCTION_DT_COLUMN_NAME));
		nwisML.setWellDepthVa(rs.getString(WELL_DEPTH_VA_COLUMN_NAME));
		nwisML.setHoleDepthVa(rs.getString(HOLE_DEPTH_VA_COLUMN_NAME));
		
		return nwisML;
	}
}
