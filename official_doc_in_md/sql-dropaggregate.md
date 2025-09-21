DROP AGGREGATE  
---  
[Prev](sql-drop-access-method.md "DROP ACCESS METHOD") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-dropcast.md "DROP CAST")  
  
* * *

## DROP AGGREGATE

DROP AGGREGATE — remove an aggregate function

## Synopsis
    
    
    DROP AGGREGATE [ IF EXISTS ] _name_ ( _aggregate_signature_ ) [, ...] [ CASCADE | RESTRICT ]
    
    where _aggregate_signature_ is:
    
    * |
    [ _argmode_ ] [ _argname_ ] _argtype_ [ , ... ] |
    [ [ _argmode_ ] [ _argname_ ] _argtype_ [ , ... ] ] ORDER BY [ _argmode_ ] [ _argname_ ] _argtype_ [ , ... ]
    

## Description

`DROP AGGREGATE` removes an existing aggregate function. To execute this command the current user must be the owner of the aggregate function. 

## Parameters

`IF EXISTS`
    

Do not throw an error if the aggregate does not exist. A notice is issued in this case. 

_`name`_
    

The name (optionally schema-qualified) of an existing aggregate function. 

_`argmode`_
    

The mode of an argument: `IN` or `VARIADIC`. If omitted, the default is `IN`. 

_`argname`_
    

The name of an argument. Note that `DROP AGGREGATE` does not actually pay any attention to argument names, since only the argument data types are needed to determine the aggregate function's identity. 

_`argtype`_
    

An input data type on which the aggregate function operates. To reference a zero-argument aggregate function, write `*` in place of the list of argument specifications. To reference an ordered-set aggregate function, write `ORDER BY` between the direct and aggregated argument specifications. 

`CASCADE`
    

Automatically drop objects that depend on the aggregate function (such as views using it), and in turn all objects that depend on those objects (see [Section 5.15](ddl-depend.md "5.15. Dependency Tracking")). 

`RESTRICT`
    

Refuse to drop the aggregate function if any objects depend on it. This is the default. 

## Notes

Alternative syntaxes for referencing ordered-set aggregates are described under [ALTER AGGREGATE](sql-alteraggregate.md "ALTER AGGREGATE"). 

## Examples

To remove the aggregate function `myavg` for type `integer`: 
    
    
    DROP AGGREGATE myavg(integer);
    

To remove the hypothetical-set aggregate function `myrank`, which takes an arbitrary list of ordering columns and a matching list of direct arguments: 
    
    
    DROP AGGREGATE myrank(VARIADIC "any" ORDER BY VARIADIC "any");
    

To remove multiple aggregate functions in one command: 
    
    
    DROP AGGREGATE myavg(integer), myavg(bigint);
    

## Compatibility

There is no `DROP AGGREGATE` statement in the SQL standard. 

## See Also

[ALTER AGGREGATE](sql-alteraggregate.md "ALTER AGGREGATE"), [CREATE AGGREGATE](sql-createaggregate.md "CREATE AGGREGATE")

* * *

[Prev](sql-drop-access-method.md "DROP ACCESS METHOD") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-dropcast.md "DROP CAST")  
---|---|---  
DROP ACCESS METHOD | [Home](index.md "PostgreSQL 17.5 Documentation")|  DROP CAST
