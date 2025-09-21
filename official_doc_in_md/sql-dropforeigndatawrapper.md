DROP FOREIGN DATA WRAPPER  
---  
[Prev](sql-dropextension.md "DROP EXTENSION") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-dropforeigntable.md "DROP FOREIGN TABLE")  
  
* * *

## DROP FOREIGN DATA WRAPPER

DROP FOREIGN DATA WRAPPER — remove a foreign-data wrapper

## Synopsis
    
    
    DROP FOREIGN DATA WRAPPER [ IF EXISTS ] _name_ [, ...] [ CASCADE | RESTRICT ]
    

## Description

`DROP FOREIGN DATA WRAPPER` removes an existing foreign-data wrapper. To execute this command, the current user must be the owner of the foreign-data wrapper. 

## Parameters

`IF EXISTS`
    

Do not throw an error if the foreign-data wrapper does not exist. A notice is issued in this case. 

_`name`_
    

The name of an existing foreign-data wrapper. 

`CASCADE`
    

Automatically drop objects that depend on the foreign-data wrapper (such as foreign tables and servers), and in turn all objects that depend on those objects (see [Section 5.15](ddl-depend.md "5.15. Dependency Tracking")). 

`RESTRICT`
    

Refuse to drop the foreign-data wrapper if any objects depend on it. This is the default. 

## Examples

Drop the foreign-data wrapper `dbi`: 
    
    
    DROP FOREIGN DATA WRAPPER dbi;
    

## Compatibility

`DROP FOREIGN DATA WRAPPER` conforms to ISO/IEC 9075-9 (SQL/MED). The `IF EXISTS` clause is a PostgreSQL extension. 

## See Also

[CREATE FOREIGN DATA WRAPPER](sql-createforeigndatawrapper.md "CREATE FOREIGN DATA WRAPPER"), [ALTER FOREIGN DATA WRAPPER](sql-alterforeigndatawrapper.md "ALTER FOREIGN DATA WRAPPER")

* * *

[Prev](sql-dropextension.md "DROP EXTENSION") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-dropforeigntable.md "DROP FOREIGN TABLE")  
---|---|---  
DROP EXTENSION | [Home](index.md "PostgreSQL 17.5 Documentation")|  DROP FOREIGN TABLE
