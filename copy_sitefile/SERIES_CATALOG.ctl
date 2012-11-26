load data
badfile 'SERIES_CATALOG.bad'
TRUNCATE INTO TABLE NWIS_WS_STAR.temp_series_catalog
fields terminated by "\t"
(
   SITE_ID   nullif SITE_ID = 'NULL',
   DATA_TYPE_CD  nullif DATA_TYPE_CD = 'NULL',
   PARM_CD   nullif PARM_CD = 'NULL',
   STAT_CD  nullif STAT_CD = 'NULL',
   LOC_NM     nullif LOC_NM = 'NULL',
   MEDIUM_GRP_CD  nullif MEDIUM_GRP_CD = 'NULL',
   PARM_GRP_CD nullif PARM_GRP_CD = 'NULL',
   SRS_ID nullif SRS_ID = 'NULL',
   ACCESS_CD  nullif ACCESS_CD = 'NULL',
   BEGIN_DATE nullif BEGIN_DATE = '0000-00-00',
   END_DATE   nullif END_DATE   = '0000-00-00',
   COUNT_NU  nullif COUNT_NU = 'NULL',
   SERIES_CATALOG_MD date "YYYY-MM-DD HH24:MI:SS"  nullif SERIES_CATALOG_MD = '0000-00-00 00:00:00')
