DROP SERVER  
---  
[Prev](sql-dropsequence.md "DROP SEQUENCE") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-dropstatistics.md "DROP STATISTICS")  
  
* * *

## DROP SERVER

DROP SERVER — remove a foreign server descriptor

## Synopsis
    
    
    DROP SERVER [ IF EXISTS ] _name_ [, ...] [ CASCADE | RESTRICT ]
    

## Description

`DROP SERVER` removes an existing foreign server descriptor. To execute this command, the current user must be the owner of the server. 

## Parameters

`IF EXISTS`
    

Do not throw an error if the server does not exist. A notice is issued in this case. 

_`name`_
    

The name of an existing server. 

`CASCADE`
    

Automatically drop objects that depend on the server (such as user mappings), and in turn all objects that depend on those objects (see [Section 5.15](ddl-depend.md "5.15. Dependency Tracking")). 

`RESTRICT`
    

Refuse to drop the server if any objects depend on it. This is the default. 

## Examples

Drop a server `foo` if it exists: 
    
    
    DROP SERVER IF EXISTS foo;
    

## Compatibility

`DROP SERVER` conforms to ISO/IEC 9075-9 (SQL/MED). The `IF EXISTS` clause is a PostgreSQL extension. 

## See Also

[CREATE SERVER](sql-createserver.md "CREATE SERVER"), [ALTER SERVER](sql-alterserver.md "ALTER SERVER")

* * *

[Prev](sql-dropsequence.md "DROP SEQUENCE") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-dropstatistics.md "DROP STATISTICS")  
---|---|---  
DROP SEQUENCE | [Home](index.md "PostgreSQL 17.5 Documentation")|  DROP STATISTICS
