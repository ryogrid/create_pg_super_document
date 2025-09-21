52.32. `pg_timezone_names`  
---  
[Prev](view-pg-timezone-abbrevs.md "52.31. pg_timezone_abbrevs") | [Up](views.md "Chapter 52. System Views")| Chapter 52. System Views| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](view-pg-user.md "52.33. pg_user")  
  
* * *

## 52.32. `pg_timezone_names` #

The view `pg_timezone_names` provides a list of time zone names that are recognized by `SET TIMEZONE`, along with their associated abbreviations, UTC offsets, and daylight-savings status. (Technically, PostgreSQL does not use UTC because leap seconds are not handled.) Unlike the abbreviations shown in [`pg_timezone_abbrevs`](view-pg-timezone-abbrevs.md "52.31. pg_timezone_abbrevs"), many of these names imply a set of daylight-savings transition date rules. Therefore, the associated information changes across local DST boundaries. The displayed information is computed based on the current value of `CURRENT_TIMESTAMP`. 

**Table 52.32.`pg_timezone_names` Columns**

Column Type  Description   
---  
`name` `text` Time zone name   
`abbrev` `text` Time zone abbreviation   
`utc_offset` `interval` Offset from UTC (positive means east of Greenwich)   
`is_dst` `bool` True if currently observing daylight savings   
  
  


* * *

[Prev](view-pg-timezone-abbrevs.md "52.31. pg_timezone_abbrevs") | [Up](views.md "Chapter 52. System Views")|  [Next](view-pg-user.md "52.33. pg_user")  
---|---|---  
52.31. `pg_timezone_abbrevs` | [Home](index.md "PostgreSQL 17.5 Documentation")|  52.33. `pg_user`
