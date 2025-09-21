DROP TABLESPACE  
---  
[Prev](sql-droptable.md "DROP TABLE") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-droptsconfig.md "DROP TEXT SEARCH CONFIGURATION")  
  
* * *

## DROP TABLESPACE

DROP TABLESPACE — remove a tablespace

## Synopsis
    
    
    DROP TABLESPACE [ IF EXISTS ] _name_
    

## Description

`DROP TABLESPACE` removes a tablespace from the system. 

A tablespace can only be dropped by its owner or a superuser. The tablespace must be empty of all database objects before it can be dropped. It is possible that objects in other databases might still reside in the tablespace even if no objects in the current database are using the tablespace. Also, if the tablespace is listed in the [temp_tablespaces](runtime-config-client.md#GUC-TEMP-TABLESPACES) setting of any active session, the `DROP` might fail due to temporary files residing in the tablespace. 

## Parameters

`IF EXISTS`
    

Do not throw an error if the tablespace does not exist. A notice is issued in this case. 

_`name`_
    

The name of a tablespace. 

## Notes

`DROP TABLESPACE` cannot be executed inside a transaction block. 

## Examples

To remove tablespace `mystuff` from the system: 
    
    
    DROP TABLESPACE mystuff;
    

## Compatibility

`DROP TABLESPACE` is a PostgreSQL extension. 

## See Also

[CREATE TABLESPACE](sql-createtablespace.md "CREATE TABLESPACE"), [ALTER TABLESPACE](sql-altertablespace.md "ALTER TABLESPACE")

* * *

[Prev](sql-droptable.md "DROP TABLE") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-droptsconfig.md "DROP TEXT SEARCH CONFIGURATION")  
---|---|---  
DROP TABLE | [Home](index.md "PostgreSQL 17.5 Documentation")|  DROP TEXT SEARCH CONFIGURATION
