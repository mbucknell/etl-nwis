package gov.acwi.wqp.etl.nwis.monitoringLocation;

public final class MLNewQueryHelper {

	public static final String EXPECTED_DATABASE_QUERY_MERGES_NEW = 
			"select agency," +
			"       site_identification_number," +
			"       site_name," +
			"       site_type," +
			"       dms_latitude," +
			"       dms_longitude," +
			"       decimal_latitude," +
			"       decimal_longitude," +
			"       latitude_longitude_method," +
			"       latitude_longitude_accuracy," +
			"       latitude_longitude_accuracy_value," +
			"       latitude_longitude_accuracy_unit," +
			"       latitude_longitude_datum," +
			"       decimal_latitude_longitude_datum," +
			"       district," +
			"       state," +
			"       county," +
			"       country," +
			"       land_net_location_description," +
			"       name_of_location_map," +
			"       scale_of_location_map," +
			"       altitude_of_guage_land_surface," +
			"       method_altitude_determined," +
			"       altitude_accuracy," +
			"       altitude_datum," +
			"       subbasin_hydrologic_unit," +
			"       drainage_basin," +
			"       topographic_setting," +
			"       flags_for_instruments_at_site," +
			"       date_of_first_construction," +
			"       date_site_established_or_inventoried," +
			"       drainage_area," +
			"       contributing_drainage_area," +
			"       time_zone_abbreviation," +
			"       site_honors_daylight_savings_time," +
			"       data_reliability," +
			"       data_other_gw_files," +
			"       national_aquifer," +
			"       local_aquifer," +
			"       local_aquifer_type," +
			"       well_depth," +
			"       hole_depth," +
			"       source_of_hole_depth," +
			"       project_numer," +
			"       site_id," +
			"       agency_cd," +
			"       site_tp_cd," +
			"       coord_meth_cd," +
			"       coord_acy_cd," +
			"       coord_datum_cd," +
			"       dec_coord_datum_cd," +
			"       district_cd," +
			"       state_cd," +
			"       county_cd," +
			"       country_cd," +
			"       alt_meth_cd," +
			"       alt_datum_cd," +
			"       huc_cd," +
			"       basin_cd," +
			"       topo_cd," +
			"       tz_cd," +
			"       local_time_fg," +
			"       reliability_cd," +
			"       gw_file_cd," +
			"       nat_aqfr_cd," +
			"       aqfr_cd," +
			"       aqfr_type_cd," +
			"       depth_src_cd," +
			"       geom," +
			"       calculated_huc_12," +
			"       nwis_host," +
			"       db_no," +
			"       site_web_cd" +
			"  from monitoring_location" +
			" where monitoring_location_id > 4";

	private MLNewQueryHelper() {}

}
