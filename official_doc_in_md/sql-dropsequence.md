DROP SEQUENCE  
---  
[Prev](sql-dropschema.md "DROP SCHEMA") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-dropserver.md "DROP SERVER")  
  
* * *

## DROP SEQUENCE

DROP SEQUENCE — remove a sequence

## Synopsis
    
    
    DROP SEQUENCE [ IF EXISTS ] _name_ [, ...] [ CASCADE | RESTRICT ]
    

## Description

`DROP SEQUENCE` removes sequence number generators. A sequence can only be dropped by its owner or a superuser. 

## Parameters

`IF EXISTS`
    

Do not throw an error if the sequence does not exist. A notice is issued in this case. 

_`name`_
    

The name (optionally schema-qualified) of a sequence. 

`CASCADE`
    

Automatically drop objects that depend on the sequence, and in turn all objects that depend on those objects (see [Section 5.15](ddl-depend.md "5.15. Dependency Tracking")). 

`RESTRICT`
    

Refuse to drop the sequence if any objects depend on it. This is the default. 

## Examples

To remove the sequence `serial`: 
    
    
    DROP SEQUENCE serial;
    

## Compatibility

`DROP SEQUENCE` conforms to the SQL standard, except that the standard only allows one sequence to be dropped per command, and apart from the `IF EXISTS` option, which is a PostgreSQL extension. 

## See Also

[CREATE SEQUENCE](sql-createsequence.md "CREATE SEQUENCE"), [ALTER SEQUENCE](sql-altersequence.md "ALTER SEQUENCE")

* * *

[Prev](sql-dropschema.md "DROP SCHEMA") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-dropserver.md "DROP SERVER")  
---|---|---  
DROP SCHEMA | [Home](index.md "PostgreSQL 17.5 Documentation")|  DROP SERVER
