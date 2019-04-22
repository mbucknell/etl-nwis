# etl\-nwis

[![Codacy Badge](https://api.codacy.com/project/badge/Grade/9cfcbdbc3cb64ca5bc2f2b90da8f63d7)](https://www.codacy.com/app/usgs_wma_dev/etl-nwis?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=NWQMC/etl-nwis&amp;utm_campaign=Badge_Grade)

ETL Water Quality Data from the NWIS System

This ETL is in the process of being transformed to a Java project using Spring Batch. For the moment we will include the README's for both.

## Spring Batch ETL

### Environment variables
Create an application.yml file in the project directory containing the following (shown are example values - they should match the values you used in creating the etlDB):

```yaml
WQP_DATABASE_HOST: <localhost>
WQP_DATABASE_PORT: <5437>
WQP_DATABASE_NAME: <wqp_db>
WQP_SCHEMA_OWNER_USERNAME: <wqp_core>
WQP_SCHEMA_OWNER_PASSWORD: <changeme>

NWIS_DATABASE_HOST: <localhost>
NWIS_DATABASE_PORT: <5437>
NWIS_DATABASE_NAME: <wqp_db>
NWIS_OWNER_USERNAME: <nwis_ws_star>
NWIS_OWNER_PASSWORD: <changeme>

# Currently I have to open a tunnel to the MYSQL db at EROS. That's why
# the address is localhost. The tunnel is opened as follows: ssh -L 1234:localhost:3306 <actual_database_address
MYSQL_DATABASE_ADDRESS: <localhost>
MYSQL_DATABASE_PORT: <1234>
MYSQL_DATABASE_NAME: <nwisweb>
MYSQL_USERNAME: <nwis_user>
MYSQL_PASSWORD: <changeme>
      
WQP_SCHEMA_NAME: <wqp>
ETL_OWNER_USERNAME: <nwis_ws_star>
```

## Ant based ETL
These scripts are run by the OWI Jenkins Job Runners. The job name is WQP\_nwis\_ETL. They follow the general OWI ETL pattern using ant to control the execution of PL/SQL scripts.

The basic flow is:

* Copy the mysql extract and oracle load scripts to the mysql server.

* Make sure they have the correct end-of-line characters.

* Extract a stable copy of the mysql data. (capture_data.sh)

* Import the data into the nwis\_ws\_star schema of the nolog database. (import_data.sh)

* Copy lookup values from the natprod database. (copyFromNatprod.sql)

	**Note:** Several code lookup values in other etls are dependent on this data.


* Copy the NEMI cross walk data. (copyFromNemi.sql)

* Copy the list of NAWQA sites from data checks. (copyFromDataChecks.sql)

* Drop the referential integrity constraints on the nwis swap tables of the wqp_core schema. (dropRI.sql)

* Drop the indexes on the nwis station swap table, populate with transformed data, and rebuild the indexes. (transformStation.sql)

* Drop the indexes on the nwis activity swap table, populate with transformed data, and rebuild the indexes. (transformActivity.sql)

* Drop the indexes on the nwis result swap table, populate with transformed data, and rebuild the indexes. (transformResult.sql)

* Drop the indexes on the nwis r\_detect\_qnt\_int swap table, populate with transformed data, and rebuild the indexes. (transformResDetectQntLmt.sql)

* Drop the indexes on the nwis summary swap tables, populate with transformed data, and rebuild the indexes. (createSummaries.sql)

* Drop the indexes on the nwis code lookup swap tables, populate with transformed data, and rebuild the indexes. (createCodes.sql)

* Add back the referential integrity constraints on the nwis swap tables of the wqp_core schema. (addRI.sql)

* Analyze the nwis swap tables of the wqp_core schema. (analyze.sql)

* Validate that rows counts and change in row counts are within the tolerated values. (validate.sql)

* Install the new data using partition exchanges. (install.sql)

* Create a new Public SRSNAMES table. (createPublicSrsnames.sql)

The translation of data is specific to this repository. The heavy lifting (indexing, RI, partition exchanges, etc.) is done using common packages in the wqp_core schema. These are defined in the schema-wqp-core repository.
