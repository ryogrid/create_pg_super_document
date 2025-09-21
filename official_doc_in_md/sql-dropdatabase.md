DROP DATABASE  
---  
[Prev](sql-dropconversion.md "DROP CONVERSION") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-dropdomain.md "DROP DOMAIN")  
  
* * *

## DROP DATABASE

DROP DATABASE — remove a database

## Synopsis
    
    
    DROP DATABASE [ IF EXISTS ] _name_ [ [ WITH ] ( _option_ [, ...] ) ]
    
    where _option_ can be:
    
        FORCE
    

## Description

`DROP DATABASE` drops a database. It removes the catalog entries for the database and deletes the directory containing the data. It can only be executed by the database owner. It cannot be executed while you are connected to the target database. (Connect to `postgres` or any other database to issue this command.) Also, if anyone else is connected to the target database, this command will fail unless you use the `FORCE` option described below. 

`DROP DATABASE` cannot be undone. Use it with care! 

## Parameters

`IF EXISTS`
    

Do not throw an error if the database does not exist. A notice is issued in this case. 

_`name`_
    

The name of the database to remove. 

`FORCE`
    

Attempt to terminate all existing connections to the target database. It doesn't terminate if prepared transactions, active logical replication slots or subscriptions are present in the target database. 

This terminates background worker connections and connections that the current user has permission to terminate with `pg_terminate_backend`, described in [Section 9.28.2](functions-admin.md#FUNCTIONS-ADMIN-SIGNAL "9.28.2. Server Signaling Functions"). If connections would remain, this command will fail. 

## Notes

`DROP DATABASE` cannot be executed inside a transaction block. 

This command cannot be executed while connected to the target database. Thus, it might be more convenient to use the program [dropdb](app-dropdb.md "dropdb") instead, which is a wrapper around this command. 

## Compatibility

There is no `DROP DATABASE` statement in the SQL standard. 

## See Also

[CREATE DATABASE](sql-createdatabase.md "CREATE DATABASE")

* * *

[Prev](sql-dropconversion.md "DROP CONVERSION") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-dropdomain.md "DROP DOMAIN")  
---|---|---  
DROP CONVERSION | [Home](index.md "PostgreSQL 17.5 Documentation")|  DROP DOMAIN
