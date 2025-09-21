51.60. `pg_ts_config_map`  
---  
[Prev](catalog-pg-ts-config.md "51.59. pg_ts_config") | [Up](catalogs.md "Chapter 51. System Catalogs")| Chapter 51. System Catalogs| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](catalog-pg-ts-dict.md "51.61. pg_ts_dict")  
  
* * *

## 51.60. `pg_ts_config_map` #

The `pg_ts_config_map` catalog contains entries showing which text search dictionaries should be consulted, and in what order, for each output token type of each text search configuration's parser. 

PostgreSQL's text search features are described at length in [Chapter 12](textsearch.md "Chapter 12. Full Text Search"). 

**Table 51.60.`pg_ts_config_map` Columns**

Column Type  Description   
---  
`mapcfg` `oid` (references [`pg_ts_config`](catalog-pg-ts-config.md "51.59. pg_ts_config").`oid`)  The OID of the [`pg_ts_config`](catalog-pg-ts-config.md "51.59. pg_ts_config") entry owning this map entry   
`maptokentype` `int4` A token type emitted by the configuration's parser   
`mapseqno` `int4` Order in which to consult this entry (lower `mapseqno`s first)   
`mapdict` `oid` (references [`pg_ts_dict`](catalog-pg-ts-dict.md "51.61. pg_ts_dict").`oid`)  The OID of the text search dictionary to consult   
  
  


* * *

[Prev](catalog-pg-ts-config.md "51.59. pg_ts_config") | [Up](catalogs.md "Chapter 51. System Catalogs")|  [Next](catalog-pg-ts-dict.md "51.61. pg_ts_dict")  
---|---|---  
51.59. `pg_ts_config` | [Home](index.md "PostgreSQL 17.5 Documentation")|  51.61. `pg_ts_dict`
