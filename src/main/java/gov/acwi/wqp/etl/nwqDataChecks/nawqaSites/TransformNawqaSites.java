package gov.acwi.wqp.etl.nwqDataChecks.nawqaSites;

import javax.sql.DataSource;

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
import org.springframework.context.annotation.Configuration;

import gov.acwi.wqp.etl.Application;

@Configuration
public class TransformNawqaSites {

    @Autowired
    @Qualifier(Application.DATASOURCE_NWQ_DATA_CHECKS_QUALIFIER)
    private DataSource dataSourceNwqDataChecks;

    @Autowired
    @Qualifier(Application.DATASOURCE_NWIS_QUALIFIER)
    private DataSource dataSourceNwis;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Bean
    public JdbcCursorItemReader<NawqaSites> nawqaSitesReader() {
        return new JdbcCursorItemReaderBuilder<NawqaSites>()
                .dataSource(dataSourceNwqDataChecks)
                .name("nawqaSitesReader")
                .sql("select * from nawqa_sites@nwq_data_checks.er.usgs.gov fetch first 2000 rows only") //TODO remove limit
                .rowMapper(new NawqaSitesRowMapper())
                .build();
    }

    @Bean
    public PassThroughItemProcessor<NawqaSites> nawqaSitesProcessor() {
        return new PassThroughItemProcessor<>();
    }

    @Bean
    public JdbcBatchItemWriter<NawqaSites> nawqaSitesWriter() {
        return new JdbcBatchItemWriterBuilder<NawqaSites>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("insert into nawqa_sites ("
                        + "nwis_host_nm, db_no, agency_cd, site_no, station_nm, station_ix, lat_va, long_va, dec_lat_va, dec_long_va, "
                        + "coord_meth_cd, coord_acy_cd, coord_datum_cd, district_cd, land_net_ds, map_nm, country_cd, state_cd, "
                        + "county_cd, mcd_cd, map_scale_fc, alt_va, alt_meth_cd, alt_acy_va, alt_datum_cd, huc_cd, agency_use_cd, "
                        + "basin_cd, site_tp_cd, topo_cd, data_types_cd, instruments_cd, site_rmks_tx, inventory_dt, drain_area_va, "
                        + "contrib_drain_area_va, tz_cd, local_time_fg, gw_file_cd, construction_dt, reliability_cd, aqfr_cd, "
                        + "nat_aqfr_cd, site_use_1_cd, site_use_2_cd, site_use_3_cd, water_use_1_cd, water_use_2_cd, water_use_3_cd, "
                        + "nat_water_use_cd, aqfr_type_cd, well_depth_va, hole_depth_va, depth_src_cd, project_no, site_web_cd, "
                        + "site_cn, site_cr, site_mn, site_md, deprecated_fg, site_ld, nawqa_site_fg, nasqan_nmn_site_fg) "
                        + "values ("
                        + ":nwisHostNm, :dbNo, :agencyCd, :siteNo, :stationNm, :stationIx, :latVa, :longVa, :decLatVa, :decLongVa, "
                        + ":coordMethCd, :coordAcyCd, :coordDatumCd, :districtCd, :landNetDs, :mapNm, :countryCd, :stateCd, "
                        + ":countyCd, :mcdCd, :mapScaleFc, :altVa, :altMethCd, :altAcyVa, :altDatumCd, :hucCd, :agencyUseCd, "
                        + ":basinCd, :siteTpCd, :topoCd, :dataTypesCd, :instrumentsCd, :siteRmksTx, :inventoryDt, :drainAreaVa, "
                        + ":contribDrainAreaVa, :tzCd, :localTimeFg, :gwFileCd, :constructionDt, :reliabilityCd, :aqfrCd, "
                        + ":natAqfrCd, :siteUse1Cd, :siteUse2Cd, :siteUse3Cd, :waterUse1Cd, :waterUse2Cd, :waterUse3Cd, "
                        + ":natWaterUseCd, :aqfrTypeCd, :wellDepthVa, :holeDepthVa, :depthSrcCd, :projectNo, :siteWebCd, "
                        + ":siteCn, :siteCr, :siteMn, :siteMd, :deprecatedFg, :siteLd, :nawqaSiteFg, :nasqanNmnSiteFg)")
                .dataSource(dataSourceNwis)
                .build();
    }
}
