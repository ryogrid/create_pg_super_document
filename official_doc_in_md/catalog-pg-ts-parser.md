51.62. `pg_ts_parser`  
---  
[Prev](catalog-pg-ts-dict.md "51.61. pg_ts_dict") | [Up](catalogs.md "Chapter 51. System Catalogs")| Chapter 51. System Catalogs| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](catalog-pg-ts-template.md "51.63. pg_ts_template")  
  
* * *

## 51.62. `pg_ts_parser` #

The `pg_ts_parser` catalog contains entries defining text search parsers. A parser is responsible for splitting input text into lexemes and assigning a token type to each lexeme. Since a parser must be implemented by C-language-level functions, creation of new parsers is restricted to database superusers. 

PostgreSQL's text search features are described at length in [Chapter 12](textsearch.md "Chapter 12. Full Text Search"). 

**Table 51.62.`pg_ts_parser` Columns**

Column Type  Description   
---  
`oid` `oid` Row identifier   
`prsname` `name` Text search parser name   
`prsnamespace` `oid` (references [`pg_namespace`](catalog-pg-namespace.md "51.32. pg_namespace").`oid`)  The OID of the namespace that contains this parser   
`prsstart` `regproc` (references [`pg_proc`](catalog-pg-proc.md "51.39. pg_proc").`oid`)  OID of the parser's startup function   
`prstoken` `regproc` (references [`pg_proc`](catalog-pg-proc.md "51.39. pg_proc").`oid`)  OID of the parser's next-token function   
`prsend` `regproc` (references [`pg_proc`](catalog-pg-proc.md "51.39. pg_proc").`oid`)  OID of the parser's shutdown function   
`prsheadline` `regproc` (references [`pg_proc`](catalog-pg-proc.md "51.39. pg_proc").`oid`)  OID of the parser's headline function (zero if none)   
`prslextype` `regproc` (references [`pg_proc`](catalog-pg-proc.md "51.39. pg_proc").`oid`)  OID of the parser's lextype function   
  
  


* * *

[Prev](catalog-pg-ts-dict.md "51.61. pg_ts_dict") | [Up](catalogs.md "Chapter 51. System Catalogs")|  [Next](catalog-pg-ts-template.md "51.63. pg_ts_template")  
---|---|---  
51.61. `pg_ts_dict` | [Home](index.md "PostgreSQL 17.5 Documentation")|  51.63. `pg_ts_template`
