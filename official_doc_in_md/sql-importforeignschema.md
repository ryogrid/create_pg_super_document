IMPORT FOREIGN SCHEMA  
---  
[Prev](sql-grant.md "GRANT") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-insert.md "INSERT")  
  
* * *

## IMPORT FOREIGN SCHEMA

IMPORT FOREIGN SCHEMA — import table definitions from a foreign server

## Synopsis
    
    
    IMPORT FOREIGN SCHEMA _remote_schema_
        [ { LIMIT TO | EXCEPT } ( _table_name_ [, ...] ) ]
        FROM SERVER _server_name_
        INTO _local_schema_
        [ OPTIONS ( _option_ '_value_ ' [, ... ] ) ]
    

## Description

`IMPORT FOREIGN SCHEMA` creates foreign tables that represent tables existing on a foreign server. The new foreign tables will be owned by the user issuing the command and are created with the correct column definitions and options to match the remote tables. 

By default, all tables and views existing in a particular schema on the foreign server are imported. Optionally, the list of tables can be limited to a specified subset, or specific tables can be excluded. The new foreign tables are all created in the target schema, which must already exist. 

To use `IMPORT FOREIGN SCHEMA`, the user must have `USAGE` privilege on the foreign server, as well as `CREATE` privilege on the target schema. 

## Parameters

 _`remote_schema`_
    

The remote schema to import from. The specific meaning of a remote schema depends on the foreign data wrapper in use. 

`LIMIT TO ( _`table_name`_ [, ...] )`
    

Import only foreign tables matching one of the given table names. Other tables existing in the foreign schema will be ignored. 

`EXCEPT ( _`table_name`_ [, ...] )`
    

Exclude specified foreign tables from the import. All tables existing in the foreign schema will be imported except the ones listed here. 

_`server_name`_
    

The foreign server to import from. 

_`local_schema`_
    

The schema in which the imported foreign tables will be created. 

`OPTIONS ( _`option`_ '_`value`_ ' [, ...] )`
    

Options to be used during the import. The allowed option names and values are specific to each foreign data wrapper. 

## Examples

Import table definitions from a remote schema `foreign_films` on server `film_server`, creating the foreign tables in local schema `films`: 
    
    
    IMPORT FOREIGN SCHEMA foreign_films
        FROM SERVER film_server INTO films;
    

As above, but import only the two tables `actors` and `directors` (if they exist): 
    
    
    IMPORT FOREIGN SCHEMA foreign_films LIMIT TO (actors, directors)
        FROM SERVER film_server INTO films;
    

## Compatibility

The `IMPORT FOREIGN SCHEMA` command conforms to the SQL standard, except that the `OPTIONS` clause is a PostgreSQL extension. 

## See Also

[CREATE FOREIGN TABLE](sql-createforeigntable.md "CREATE FOREIGN TABLE"), [CREATE SERVER](sql-createserver.md "CREATE SERVER")

* * *

[Prev](sql-grant.md "GRANT") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-insert.md "INSERT")  
---|---|---  
GRANT | [Home](index.md "PostgreSQL 17.5 Documentation")|  INSERT
