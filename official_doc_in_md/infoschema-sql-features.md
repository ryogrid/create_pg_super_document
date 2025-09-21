35.48. `sql_features`  
---  
[Prev](infoschema-sequences.md "35.47. sequences") | [Up](information-schema.md "Chapter 35. The Information Schema")| Chapter 35. The Information Schema| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](infoschema-sql-implementation-info.md "35.49. sql_implementation_info")  
  
* * *

## 35.48. `sql_features` #

The table `sql_features` contains information about which formal features defined in the SQL standard are supported by PostgreSQL. This is the same information that is presented in [Appendix D](features.md "Appendix D. SQL Conformance"). There you can also find some additional background information. 

**Table 35.46.`sql_features` Columns**

Column Type  Description   
---  
`feature_id` `character_data` Identifier string of the feature   
`feature_name` `character_data` Descriptive name of the feature   
`sub_feature_id` `character_data` Identifier string of the subfeature, or a zero-length string if not a subfeature   
`sub_feature_name` `character_data` Descriptive name of the subfeature, or a zero-length string if not a subfeature   
`is_supported` `yes_or_no` `YES` if the feature is fully supported by the current version of PostgreSQL, `NO` if not   
`is_verified_by` `character_data` Always null, since the PostgreSQL development group does not perform formal testing of feature conformance   
`comments` `character_data` Possibly a comment about the supported status of the feature   
  
  


* * *

[Prev](infoschema-sequences.md "35.47. sequences") | [Up](information-schema.md "Chapter 35. The Information Schema")|  [Next](infoschema-sql-implementation-info.md "35.49. sql_implementation_info")  
---|---|---  
35.47. `sequences` | [Home](index.md "PostgreSQL 17.5 Documentation")|  35.49. `sql_implementation_info`
