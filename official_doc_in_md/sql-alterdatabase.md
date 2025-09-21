ALTER DATABASE  
---  
[Prev](sql-alterconversion.md "ALTER CONVERSION") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-alterdefaultprivileges.md "ALTER DEFAULT PRIVILEGES")  
  
* * *

## ALTER DATABASE

ALTER DATABASE — change a database

## Synopsis
    
    
    ALTER DATABASE _name_ [ [ WITH ] _option_ [ ... ] ]
    
    where _option_ can be:
    
        ALLOW_CONNECTIONS _allowconn_
        CONNECTION LIMIT _connlimit_
        IS_TEMPLATE _istemplate_
    
    ALTER DATABASE _name_ RENAME TO _new_name_
    
    ALTER DATABASE _name_ OWNER TO { _new_owner_ | CURRENT_ROLE | CURRENT_USER | SESSION_USER }
    
    ALTER DATABASE _name_ SET TABLESPACE _new_tablespace_
    
    ALTER DATABASE _name_ REFRESH COLLATION VERSION
    
    ALTER DATABASE _name_ SET _configuration_parameter_ { TO | = } { _value_ | DEFAULT }
    ALTER DATABASE _name_ SET _configuration_parameter_ FROM CURRENT
    ALTER DATABASE _name_ RESET _configuration_parameter_
    ALTER DATABASE _name_ RESET ALL
    

## Description

`ALTER DATABASE` changes the attributes of a database. 

The first form changes certain per-database settings. (See below for details.) Only the database owner or a superuser can change these settings. 

The second form changes the name of the database. Only the database owner or a superuser can rename a database; non-superuser owners must also have the `CREATEDB` privilege. The current database cannot be renamed. (Connect to a different database if you need to do that.) 

The third form changes the owner of the database. To alter the owner, you must be able to `SET ROLE` to the new owning role, and you must have the `CREATEDB` privilege. (Note that superusers have all these privileges automatically.) 

The fourth form changes the default tablespace of the database. Only the database owner or a superuser can do this; you must also have create privilege for the new tablespace. This command physically moves any tables or indexes in the database's old default tablespace to the new tablespace. The new default tablespace must be empty for this database, and no one can be connected to the database. Tables and indexes in non-default tablespaces are unaffected. 

The remaining forms change the session default for a run-time configuration variable for a PostgreSQL database. Whenever a new session is subsequently started in that database, the specified value becomes the session default value. The database-specific default overrides whatever setting is present in `postgresql.conf` or has been received from the `postgres` command line. Only the database owner or a superuser can change the session defaults for a database. Certain variables cannot be set this way, or can only be set by a superuser. 

## Parameters

 _`name`_
    

The name of the database whose attributes are to be altered. 

_`allowconn`_
    

If false then no one can connect to this database. 

_`connlimit`_
    

How many concurrent connections can be made to this database. -1 means no limit. 

_`istemplate`_
    

If true, then this database can be cloned by any user with `CREATEDB` privileges; if false, then only superusers or the owner of the database can clone it. 

_`new_name`_
    

The new name of the database. 

_`new_owner`_
    

The new owner of the database. 

_`new_tablespace`_
    

The new default tablespace of the database. 

This form of the command cannot be executed inside a transaction block. 

`REFRESH COLLATION VERSION`
    

Update the database collation version. See [Notes](sql-altercollation.md#SQL-ALTERCOLLATION-NOTES "Notes") for background. 

_`configuration_parameter`_  
 _`value`_
    

Set this database's session default for the specified configuration parameter to the given value. If _`value`_ is `DEFAULT` or, equivalently, `RESET` is used, the database-specific setting is removed, so the system-wide default setting will be inherited in new sessions. Use `RESET ALL` to clear all database-specific settings. `SET FROM CURRENT` saves the session's current value of the parameter as the database-specific value. 

See [SET](sql-set.md "SET") and [Chapter 19](runtime-config.md "Chapter 19. Server Configuration") for more information about allowed parameter names and values. 

## Notes

It is also possible to tie a session default to a specific role rather than to a database; see [ALTER ROLE](sql-alterrole.md "ALTER ROLE"). Role-specific settings override database-specific ones if there is a conflict. 

## Examples

To disable index scans by default in the database `test`: 
    
    
    ALTER DATABASE test SET enable_indexscan TO off;
    

## Compatibility

The `ALTER DATABASE` statement is a PostgreSQL extension. 

## See Also

[CREATE DATABASE](sql-createdatabase.md "CREATE DATABASE"), [DROP DATABASE](sql-dropdatabase.md "DROP DATABASE"), [SET](sql-set.md "SET"), [CREATE TABLESPACE](sql-createtablespace.md "CREATE TABLESPACE")

* * *

[Prev](sql-alterconversion.md "ALTER CONVERSION") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-alterdefaultprivileges.md "ALTER DEFAULT PRIVILEGES")  
---|---|---  
ALTER CONVERSION | [Home](index.md "PostgreSQL 17.5 Documentation")|  ALTER DEFAULT PRIVILEGES
