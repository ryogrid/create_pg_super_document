51.22. `pg_extension`  
---  
[Prev](catalog-pg-event-trigger.md "51.21. pg_event_trigger") | [Up](catalogs.md "Chapter 51. System Catalogs")| Chapter 51. System Catalogs| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](catalog-pg-foreign-data-wrapper.md "51.23. pg_foreign_data_wrapper")  
  
* * *

## 51.22. `pg_extension` #

The catalog `pg_extension` stores information about the installed extensions. See [Section 36.17](extend-extensions.md "36.17. Packaging Related Objects into an Extension") for details about extensions. 

**Table 51.22.`pg_extension` Columns**

Column Type  Description   
---  
`oid` `oid` Row identifier   
`extname` `name` Name of the extension   
`extowner` `oid` (references [`pg_authid`](catalog-pg-authid.md "51.8. pg_authid").`oid`)  Owner of the extension   
`extnamespace` `oid` (references [`pg_namespace`](catalog-pg-namespace.md "51.32. pg_namespace").`oid`)  Schema containing the extension's exported objects   
`extrelocatable` `bool` True if extension can be relocated to another schema   
`extversion` `text` Version name for the extension   
`extconfig` `oid[]` (references [`pg_class`](catalog-pg-class.md "51.11. pg_class").`oid`)  Array of `regclass` OIDs for the extension's configuration table(s), or `NULL` if none   
`extcondition` `text[]` Array of `WHERE`-clause filter conditions for the extension's configuration table(s), or `NULL` if none   
  
  


Note that unlike most catalogs with a “namespace” column, `extnamespace` is not meant to imply that the extension belongs to that schema. Extension names are never schema-qualified. Rather, `extnamespace` indicates the schema that contains most or all of the extension's objects. If `extrelocatable` is true, then this schema must in fact contain all schema-qualifiable objects belonging to the extension. 

* * *

[Prev](catalog-pg-event-trigger.md "51.21. pg_event_trigger") | [Up](catalogs.md "Chapter 51. System Catalogs")|  [Next](catalog-pg-foreign-data-wrapper.md "51.23. pg_foreign_data_wrapper")  
---|---|---  
51.21. `pg_event_trigger` | [Home](index.md "PostgreSQL 17.5 Documentation")|  51.23. `pg_foreign_data_wrapper`
