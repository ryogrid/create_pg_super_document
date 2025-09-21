DROP OPERATOR CLASS  
---  
[Prev](sql-dropoperator.md "DROP OPERATOR") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-dropopfamily.md "DROP OPERATOR FAMILY")  
  
* * *

## DROP OPERATOR CLASS

DROP OPERATOR CLASS — remove an operator class

## Synopsis
    
    
    DROP OPERATOR CLASS [ IF EXISTS ] _name_ USING _index_method_ [ CASCADE | RESTRICT ]
    

## Description

`DROP OPERATOR CLASS` drops an existing operator class. To execute this command you must be the owner of the operator class. 

`DROP OPERATOR CLASS` does not drop any of the operators or functions referenced by the class. If there are any indexes depending on the operator class, you will need to specify `CASCADE` for the drop to complete. 

## Parameters

`IF EXISTS`
    

Do not throw an error if the operator class does not exist. A notice is issued in this case. 

_`name`_
    

The name (optionally schema-qualified) of an existing operator class. 

_`index_method`_
    

The name of the index access method the operator class is for. 

`CASCADE`
    

Automatically drop objects that depend on the operator class (such as indexes), and in turn all objects that depend on those objects (see [Section 5.15](ddl-depend.md "5.15. Dependency Tracking")). 

`RESTRICT`
    

Refuse to drop the operator class if any objects depend on it. This is the default. 

## Notes

`DROP OPERATOR CLASS` will not drop the operator family containing the class, even if there is nothing else left in the family (in particular, in the case where the family was implicitly created by `CREATE OPERATOR CLASS`). An empty operator family is harmless, but for the sake of tidiness you might wish to remove the family with `DROP OPERATOR FAMILY`; or perhaps better, use `DROP OPERATOR FAMILY` in the first place. 

## Examples

Remove the B-tree operator class `widget_ops`: 
    
    
    DROP OPERATOR CLASS widget_ops USING btree;
    

This command will not succeed if there are any existing indexes that use the operator class. Add `CASCADE` to drop such indexes along with the operator class. 

## Compatibility

There is no `DROP OPERATOR CLASS` statement in the SQL standard. 

## See Also

[ALTER OPERATOR CLASS](sql-alteropclass.md "ALTER OPERATOR CLASS"), [CREATE OPERATOR CLASS](sql-createopclass.md "CREATE OPERATOR CLASS"), [DROP OPERATOR FAMILY](sql-dropopfamily.md "DROP OPERATOR FAMILY")

* * *

[Prev](sql-dropoperator.md "DROP OPERATOR") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-dropopfamily.md "DROP OPERATOR FAMILY")  
---|---|---  
DROP OPERATOR | [Home](index.md "PostgreSQL 17.5 Documentation")|  DROP OPERATOR FAMILY
