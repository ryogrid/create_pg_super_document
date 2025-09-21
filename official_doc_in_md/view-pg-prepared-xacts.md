52.16. `pg_prepared_xacts`  
---  
[Prev](view-pg-prepared-statements.md "52.15. pg_prepared_statements") | [Up](views.md "Chapter 52. System Views")| Chapter 52. System Views| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](view-pg-publication-tables.md "52.17. pg_publication_tables")  
  
* * *

## 52.16. `pg_prepared_xacts` #

The view `pg_prepared_xacts` displays information about transactions that are currently prepared for two-phase commit (see [PREPARE TRANSACTION](sql-prepare-transaction.md "PREPARE TRANSACTION") for details). 

`pg_prepared_xacts` contains one row per prepared transaction. An entry is removed when the transaction is committed or rolled back. 

**Table 52.16.`pg_prepared_xacts` Columns**

Column Type  Description   
---  
`transaction` `xid` Numeric transaction identifier of the prepared transaction   
`gid` `text` Global transaction identifier that was assigned to the transaction   
`prepared` `timestamptz` Time at which the transaction was prepared for commit   
`owner` `name` (references [`pg_authid`](catalog-pg-authid.md "51.8. pg_authid").`rolname`)  Name of the user that executed the transaction   
`database` `name` (references [`pg_database`](catalog-pg-database.md "51.15. pg_database").`datname`)  Name of the database in which the transaction was executed   
  
  


When the `pg_prepared_xacts` view is accessed, the internal transaction manager data structures are momentarily locked, and a copy is made for the view to display. This ensures that the view produces a consistent set of results, while not blocking normal operations longer than necessary. Nonetheless there could be some impact on database performance if this view is frequently accessed. 

* * *

[Prev](view-pg-prepared-statements.md "52.15. pg_prepared_statements") | [Up](views.md "Chapter 52. System Views")|  [Next](view-pg-publication-tables.md "52.17. pg_publication_tables")  
---|---|---  
52.15. `pg_prepared_statements` | [Home](index.md "PostgreSQL 17.5 Documentation")|  52.17. `pg_publication_tables`
