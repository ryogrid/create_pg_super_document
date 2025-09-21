DROP PUBLICATION  
---  
[Prev](sql-dropprocedure.md "DROP PROCEDURE") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-droprole.md "DROP ROLE")  
  
* * *

## DROP PUBLICATION

DROP PUBLICATION — remove a publication

## Synopsis
    
    
    DROP PUBLICATION [ IF EXISTS ] _name_ [, ...] [ CASCADE | RESTRICT ]
    

## Description

`DROP PUBLICATION` removes an existing publication from the database. 

A publication can only be dropped by its owner or a superuser. 

## Parameters

`IF EXISTS`
    

Do not throw an error if the publication does not exist. A notice is issued in this case. 

_`name`_
    

The name of an existing publication. 

`CASCADE`  
`RESTRICT`
    

These key words do not have any effect, since there are no dependencies on publications. 

## Examples

Drop a publication: 
    
    
    DROP PUBLICATION mypublication;
    

## Compatibility

`DROP PUBLICATION` is a PostgreSQL extension. 

## See Also

[CREATE PUBLICATION](sql-createpublication.md "CREATE PUBLICATION"), [ALTER PUBLICATION](sql-alterpublication.md "ALTER PUBLICATION")

* * *

[Prev](sql-dropprocedure.md "DROP PROCEDURE") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-droprole.md "DROP ROLE")  
---|---|---  
DROP PROCEDURE | [Home](index.md "PostgreSQL 17.5 Documentation")|  DROP ROLE
