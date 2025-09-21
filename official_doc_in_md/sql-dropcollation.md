DROP COLLATION  
---  
[Prev](sql-dropcast.md "DROP CAST") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-dropconversion.md "DROP CONVERSION")  
  
* * *

## DROP COLLATION

DROP COLLATION — remove a collation

## Synopsis
    
    
    DROP COLLATION [ IF EXISTS ] _name_ [ CASCADE | RESTRICT ]
    

## Description

`DROP COLLATION` removes a previously defined collation. To be able to drop a collation, you must own the collation. 

## Parameters

`IF EXISTS`
    

Do not throw an error if the collation does not exist. A notice is issued in this case. 

_`name`_
    

The name of the collation. The collation name can be schema-qualified. 

`CASCADE`
    

Automatically drop objects that depend on the collation, and in turn all objects that depend on those objects (see [Section 5.15](ddl-depend.md "5.15. Dependency Tracking")). 

`RESTRICT`
    

Refuse to drop the collation if any objects depend on it. This is the default. 

## Examples

To drop the collation named `german`: 
    
    
    DROP COLLATION german;
    

## Compatibility

The `DROP COLLATION` command conforms to the SQL standard, apart from the `IF EXISTS` option, which is a PostgreSQL extension. 

## See Also

[ALTER COLLATION](sql-altercollation.md "ALTER COLLATION"), [CREATE COLLATION](sql-createcollation.md "CREATE COLLATION")

* * *

[Prev](sql-dropcast.md "DROP CAST") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-dropconversion.md "DROP CONVERSION")  
---|---|---  
DROP CAST | [Home](index.md "PostgreSQL 17.5 Documentation")|  DROP CONVERSION
