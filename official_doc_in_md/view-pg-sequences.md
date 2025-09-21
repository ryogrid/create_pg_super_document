52.23. `pg_sequences`  
---  
[Prev](view-pg-seclabels.md "52.22. pg_seclabels") | [Up](views.md "Chapter 52. System Views")| Chapter 52. System Views| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](view-pg-settings.md "52.24. pg_settings")  
  
* * *

## 52.23. `pg_sequences` #

The view `pg_sequences` provides access to useful information about each sequence in the database. 

**Table 52.23.`pg_sequences` Columns**

Column Type  Description   
---  
`schemaname` `name` (references [`pg_namespace`](catalog-pg-namespace.md "51.32. pg_namespace").`nspname`)  Name of schema containing sequence   
`sequencename` `name` (references [`pg_class`](catalog-pg-class.md "51.11. pg_class").`relname`)  Name of sequence   
`sequenceowner` `name` (references [`pg_authid`](catalog-pg-authid.md "51.8. pg_authid").`rolname`)  Name of sequence's owner   
`data_type` `regtype` (references [`pg_type`](catalog-pg-type.md "51.64. pg_type").`oid`)  Data type of the sequence   
`start_value` `int8` Start value of the sequence   
`min_value` `int8` Minimum value of the sequence   
`max_value` `int8` Maximum value of the sequence   
`increment_by` `int8` Increment value of the sequence   
`cycle` `bool` Whether the sequence cycles   
`cache_size` `int8` Cache size of the sequence   
`last_value` `int8` The last sequence value written to disk. If caching is used, this value can be greater than the last value handed out from the sequence.   
  
  


The `last_value` column will read as null if any of the following are true: 

  * The sequence has not been read from yet. 

  * The current user does not have `USAGE` or `SELECT` privilege on the sequence. 

  * The sequence is unlogged and the server is a standby. 




* * *

[Prev](view-pg-seclabels.md "52.22. pg_seclabels") | [Up](views.md "Chapter 52. System Views")|  [Next](view-pg-settings.md "52.24. pg_settings")  
---|---|---  
52.22. `pg_seclabels` | [Home](index.md "PostgreSQL 17.5 Documentation")|  52.24. `pg_settings`
