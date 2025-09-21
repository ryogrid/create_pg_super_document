51.15. `pg_database`  
---  
[Prev](catalog-pg-conversion.md "51.14. pg_conversion") | [Up](catalogs.md "Chapter 51. System Catalogs")| Chapter 51. System Catalogs| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](catalog-pg-db-role-setting.md "51.16. pg_db_role_setting")  
  
* * *

## 51.15. `pg_database` #

The catalog `pg_database` stores information about the available databases. Databases are created with the [`CREATE DATABASE`](sql-createdatabase.md "CREATE DATABASE") command. Consult [Chapter 22](managing-databases.md "Chapter 22. Managing Databases") for details about the meaning of some of the parameters. 

Unlike most system catalogs, `pg_database` is shared across all databases of a cluster: there is only one copy of `pg_database` per cluster, not one per database. 

**Table 51.15.`pg_database` Columns**

Column Type  Description   
---  
`oid` `oid` Row identifier   
`datname` `name` Database name   
`datdba` `oid` (references [`pg_authid`](catalog-pg-authid.md "51.8. pg_authid").`oid`)  Owner of the database, usually the user who created it   
`encoding` `int4` Character encoding for this database ([`pg_encoding_to_char()`](functions-info.md#PG-ENCODING-TO-CHAR) can translate this number to the encoding name)   
`datlocprovider` `char` Locale provider for this database: `b` = builtin, `c` = libc, `i` = icu   
`datistemplate` `bool` If true, then this database can be cloned by any user with `CREATEDB` privileges; if false, then only superusers or the owner of the database can clone it.   
`datallowconn` `bool` If false then no one can connect to this database. This is used to protect the `template0` database from being altered.   
`dathasloginevt` `bool` Indicates that there are login event triggers defined for this database. This flag is used to avoid extra lookups on the `pg_event_trigger` table during each backend startup. This flag is used internally by PostgreSQL and should not be manually altered or read for monitoring purposes.   
`datconnlimit` `int4` Sets maximum number of concurrent connections that can be made to this database. -1 means no limit, -2 indicates the database is invalid.   
`datfrozenxid` `xid` All transaction IDs before this one have been replaced with a permanent (“frozen”) transaction ID in this database. This is used to track whether the database needs to be vacuumed in order to prevent transaction ID wraparound or to allow `pg_xact` to be shrunk. It is the minimum of the per-table [`pg_class`](catalog-pg-class.md "51.11. pg_class").`relfrozenxid` values.   
`datminmxid` `xid` All multixact IDs before this one have been replaced with a transaction ID in this database. This is used to track whether the database needs to be vacuumed in order to prevent multixact ID wraparound or to allow `pg_multixact` to be shrunk. It is the minimum of the per-table [`pg_class`](catalog-pg-class.md "51.11. pg_class").`relminmxid` values.   
`dattablespace` `oid` (references [`pg_tablespace`](catalog-pg-tablespace.md "51.56. pg_tablespace").`oid`)  The default tablespace for the database. Within this database, all tables for which [`pg_class`](catalog-pg-class.md "51.11. pg_class").`reltablespace` is zero will be stored in this tablespace; in particular, all the non-shared system catalogs will be there.   
`datcollate` `text` LC_COLLATE for this database   
`datctype` `text` LC_CTYPE for this database   
`datlocale` `text` Collation provider locale name for this database. If the provider is `libc`, `datlocale` is `NULL`; `datcollate` and `datctype` are used instead.   
`daticurules` `text` ICU collation rules for this database   
`datcollversion` `text` Provider-specific version of the collation. This is recorded when the database is created and then checked when it is used, to detect changes in the collation definition that could lead to data corruption.   
`datacl` `aclitem[]` Access privileges; see [Section 5.8](ddl-priv.md "5.8. Privileges") for details   
  
  


* * *

[Prev](catalog-pg-conversion.md "51.14. pg_conversion") | [Up](catalogs.md "Chapter 51. System Catalogs")|  [Next](catalog-pg-db-role-setting.md "51.16. pg_db_role_setting")  
---|---|---  
51.14. `pg_conversion` | [Home](index.md "PostgreSQL 17.5 Documentation")|  51.16. `pg_db_role_setting`
