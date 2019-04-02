# etl\-nwis

ETL Water Quality Data from the NWIS System

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
