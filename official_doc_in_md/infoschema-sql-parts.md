35.50. `sql_parts`  
---  
[Prev](infoschema-sql-implementation-info.md "35.49. sql_implementation_info") | [Up](information-schema.md "Chapter 35. The Information Schema")| Chapter 35. The Information Schema| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](infoschema-sql-sizing.md "35.51. sql_sizing")  
  
* * *

## 35.50. `sql_parts` #

The table `sql_parts` contains information about which of the several parts of the SQL standard are supported by PostgreSQL. 

**Table 35.48.`sql_parts` Columns**

Column Type  Description   
---  
`feature_id` `character_data` An identifier string containing the number of the part   
`feature_name` `character_data` Descriptive name of the part   
`is_supported` `yes_or_no` `YES` if the part is fully supported by the current version of PostgreSQL, `NO` if not   
`is_verified_by` `character_data` Always null, since the PostgreSQL development group does not perform formal testing of feature conformance   
`comments` `character_data` Possibly a comment about the supported status of the part   
  
  


* * *

[Prev](infoschema-sql-implementation-info.md "35.49. sql_implementation_info") | [Up](information-schema.md "Chapter 35. The Information Schema")|  [Next](infoschema-sql-sizing.md "35.51. sql_sizing")  
---|---|---  
35.49. `sql_implementation_info` | [Home](index.md "PostgreSQL 17.5 Documentation")|  35.51. `sql_sizing`
