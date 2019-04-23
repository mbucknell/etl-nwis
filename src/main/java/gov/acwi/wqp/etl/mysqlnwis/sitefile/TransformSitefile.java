package gov.acwi.wqp.etl.mysqlnwis.sitefile;

import gov.acwi.wqp.etl.Application;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.support.PassThroughItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;

@Component
public class TransformSitefile {

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier(Application.DATASOURCE_MYSQLNWIS_QUALIFER)
	private DataSource dataSourceMysqlnwis;

	@Autowired
	@Qualifier(Application.DATASOURCE_NWIS_QUALIFIER)
	private DataSource dataSourceNwis;
	
	@Bean
	public JdbcCursorItemReader<Sitefile> sitefileReader() {
		return new JdbcCursorItemReaderBuilder<Sitefile>()
				.dataSource(dataSourceMysqlnwis)
				.name("mysqlQwSampleReader")
				.sql("select * from SITEFILE limit 2000") //TODO remove limit
				.rowMapper(new SitefileRowMapper())
				.build();
	}
	
	@Bean
	public PassThroughItemProcessor<Sitefile> sitefileProcessor() {
		return new PassThroughItemProcessor<>();
	}
	
	@Bean
	public JdbcBatchItemWriter<Sitefile> sitefileWriter() {
		return new JdbcBatchItemWriterBuilder<Sitefile>()
				.itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
				.sql("insert into sitefile ("
						+ "site_id, agency_cd, site_no, nwis_host, db_no, station_nm, "
						+ "dec_lat_va, dec_long_va, coord_meth_cd, coord_acy_cd, district_cd, "
						+ "country_cd, state_cd, county_cd, land_net_ds, map_scale_fc, "
						+ "alt_va, alt_meth_cd, alt_acy_va, alt_datum_cd, huc_cd, basin_cd, "
						+ "site_tp_cd, site_rmks_tx, drain_area_va, contrib_drain_area_va, "
						+ "construction_dt, aqfr_type_cd, aqfr_cd, nat_aqfr_cd, well_depth_va, "
						+ "hole_depth_va, site_web_cd, dec_coord_datum_cd, site_cn, "
						+ "site_cr, site_mn, site_md) VALUES ("
						+ ":siteId, :agencyCd, :siteNo, :nwisHost, :dbNo, :stationNm, "
						+ ":decLatVa, :decLongVa, :coordMethCd, :coordAcyCd, :districtCd, "
						+ ":countryCd, :stateCd, :countyCd, :landNetDs, :mapScaleFc, "
						+ ":altVa, :altMethCd, :altAcyVa, :altDatumCd, :hucCd, :basinCd, "
						+ ":siteTpCd, :siteRmksTx, :drainAreaVa, :contribDrainAreaVa, "
						+ ":constructionDt, :aqfrTypeCd, :aqfrCd, :natAqfrCd, :wellDepthVa, "
						+ ":holeDepthVa, :siteWebCd, :decCoordDatumCd, :siteCn, "
						+ ":siteCr, :siteMn, :siteMd)")
				.dataSource(dataSourceNwis)
				.build();
	}
}
