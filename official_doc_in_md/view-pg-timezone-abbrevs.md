52.31. `pg_timezone_abbrevs`  
---  
[Prev](view-pg-tables.md "52.30. pg_tables") | [Up](views.md "Chapter 52. System Views")| Chapter 52. System Views| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](view-pg-timezone-names.md "52.32. pg_timezone_names")  
  
* * *

## 52.31. `pg_timezone_abbrevs` #

The view `pg_timezone_abbrevs` provides a list of time zone abbreviations that are currently recognized by the datetime input routines. The contents of this view change when the [timezone_abbreviations](runtime-config-client.md#GUC-TIMEZONE-ABBREVIATIONS) run-time parameter is modified. 

**Table 52.31.`pg_timezone_abbrevs` Columns**

Column Type  Description   
---  
`abbrev` `text` Time zone abbreviation   
`utc_offset` `interval` Offset from UTC (positive means east of Greenwich)   
`is_dst` `bool` True if this is a daylight-savings abbreviation   
  
  


While most timezone abbreviations represent fixed offsets from UTC, there are some that have historically varied in value (see [Section B.4](datetime-config-files.md "B.4. Date/Time Configuration Files") for more information). In such cases this view presents their current meaning. 

* * *

[Prev](view-pg-tables.md "52.30. pg_tables") | [Up](views.md "Chapter 52. System Views")|  [Next](view-pg-timezone-names.md "52.32. pg_timezone_names")  
---|---|---  
52.30. `pg_tables` | [Home](index.md "PostgreSQL 17.5 Documentation")|  52.32. `pg_timezone_names`
