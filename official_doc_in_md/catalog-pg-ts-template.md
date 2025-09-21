51.63. `pg_ts_template`  
---  
[Prev](catalog-pg-ts-parser.md "51.62. pg_ts_parser") | [Up](catalogs.md "Chapter 51. System Catalogs")| Chapter 51. System Catalogs| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](catalog-pg-type.md "51.64. pg_type")  
  
* * *

## 51.63. `pg_ts_template` #

The `pg_ts_template` catalog contains entries defining text search templates. A template is the implementation skeleton for a class of text search dictionaries. Since a template must be implemented by C-language-level functions, creation of new templates is restricted to database superusers. 

PostgreSQL's text search features are described at length in [Chapter 12](textsearch.md "Chapter 12. Full Text Search"). 

**Table 51.63.`pg_ts_template` Columns**

Column Type  Description   
---  
`oid` `oid` Row identifier   
`tmplname` `name` Text search template name   
`tmplnamespace` `oid` (references [`pg_namespace`](catalog-pg-namespace.md "51.32. pg_namespace").`oid`)  The OID of the namespace that contains this template   
`tmplinit` `regproc` (references [`pg_proc`](catalog-pg-proc.md "51.39. pg_proc").`oid`)  OID of the template's initialization function (zero if none)   
`tmpllexize` `regproc` (references [`pg_proc`](catalog-pg-proc.md "51.39. pg_proc").`oid`)  OID of the template's lexize function   
  
  


* * *

[Prev](catalog-pg-ts-parser.md "51.62. pg_ts_parser") | [Up](catalogs.md "Chapter 51. System Catalogs")|  [Next](catalog-pg-type.md "51.64. pg_type")  
---|---|---  
51.62. `pg_ts_parser` | [Home](index.md "PostgreSQL 17.5 Documentation")|  51.64. `pg_type`
