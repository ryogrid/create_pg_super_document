51.59. `pg_ts_config`  
---  
[Prev](catalog-pg-trigger.md "51.58. pg_trigger") | [Up](catalogs.md "Chapter 51. System Catalogs")| Chapter 51. System Catalogs| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](catalog-pg-ts-config-map.md "51.60. pg_ts_config_map")  
  
* * *

## 51.59. `pg_ts_config` #

The `pg_ts_config` catalog contains entries representing text search configurations. A configuration specifies a particular text search parser and a list of dictionaries to use for each of the parser's output token types. The parser is shown in the `pg_ts_config` entry, but the token-to-dictionary mapping is defined by subsidiary entries in [`pg_ts_config_map`](catalog-pg-ts-config-map.md "51.60. pg_ts_config_map"). 

PostgreSQL's text search features are described at length in [Chapter 12](textsearch.md "Chapter 12. Full Text Search"). 

**Table 51.59.`pg_ts_config` Columns**

Column Type  Description   
---  
`oid` `oid` Row identifier   
`cfgname` `name` Text search configuration name   
`cfgnamespace` `oid` (references [`pg_namespace`](catalog-pg-namespace.md "51.32. pg_namespace").`oid`)  The OID of the namespace that contains this configuration   
`cfgowner` `oid` (references [`pg_authid`](catalog-pg-authid.md "51.8. pg_authid").`oid`)  Owner of the configuration   
`cfgparser` `oid` (references [`pg_ts_parser`](catalog-pg-ts-parser.md "51.62. pg_ts_parser").`oid`)  The OID of the text search parser for this configuration   
  
  


* * *

[Prev](catalog-pg-trigger.md "51.58. pg_trigger") | [Up](catalogs.md "Chapter 51. System Catalogs")|  [Next](catalog-pg-ts-config-map.md "51.60. pg_ts_config_map")  
---|---|---  
51.58. `pg_trigger` | [Home](index.md "PostgreSQL 17.5 Documentation")|  51.60. `pg_ts_config_map`
