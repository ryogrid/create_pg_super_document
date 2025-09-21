DROP CAST  
---  
[Prev](sql-dropaggregate.md "DROP AGGREGATE") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-dropcollation.md "DROP COLLATION")  
  
* * *

## DROP CAST

DROP CAST — remove a cast

## Synopsis
    
    
    DROP CAST [ IF EXISTS ] (_source_type_ AS _target_type_) [ CASCADE | RESTRICT ]
    

## Description

`DROP CAST` removes a previously defined cast. 

To be able to drop a cast, you must own the source or the target data type. These are the same privileges that are required to create a cast. 

## Parameters

`IF EXISTS`
    

Do not throw an error if the cast does not exist. A notice is issued in this case. 

_`source_type`_
    

The name of the source data type of the cast. 

_`target_type`_
    

The name of the target data type of the cast. 

`CASCADE`  
`RESTRICT`
    

These key words do not have any effect, since there are no dependencies on casts. 

## Examples

To drop the cast from type `text` to type `int`: 
    
    
    DROP CAST (text AS int);
    

## Compatibility

The `DROP CAST` command conforms to the SQL standard. 

## See Also

[CREATE CAST](sql-createcast.md "CREATE CAST")

* * *

[Prev](sql-dropaggregate.md "DROP AGGREGATE") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-dropcollation.md "DROP COLLATION")  
---|---|---  
DROP AGGREGATE | [Home](index.md "PostgreSQL 17.5 Documentation")|  DROP COLLATION
