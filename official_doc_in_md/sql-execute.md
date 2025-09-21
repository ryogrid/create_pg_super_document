EXECUTE  
---  
[Prev](sql-end.md "END") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-explain.md "EXPLAIN")  
  
* * *

## EXECUTE

EXECUTE — execute a prepared statement

## Synopsis
    
    
    EXECUTE _name_ [ ( _parameter_ [, ...] ) ]
    

## Description

`EXECUTE` is used to execute a previously prepared statement. Since prepared statements only exist for the duration of a session, the prepared statement must have been created by a `PREPARE` statement executed earlier in the current session. 

If the `PREPARE` statement that created the statement specified some parameters, a compatible set of parameters must be passed to the `EXECUTE` statement, or else an error is raised. Note that (unlike functions) prepared statements are not overloaded based on the type or number of their parameters; the name of a prepared statement must be unique within a database session. 

For more information on the creation and usage of prepared statements, see [PREPARE](sql-prepare.md "PREPARE"). 

## Parameters

 _`name`_
    

The name of the prepared statement to execute. 

_`parameter`_
    

The actual value of a parameter to the prepared statement. This must be an expression yielding a value that is compatible with the data type of this parameter, as was determined when the prepared statement was created. 

## Outputs

The command tag returned by `EXECUTE` is that of the prepared statement, and not `EXECUTE`. 

## Examples

Examples are given in [Examples](sql-prepare.md#SQL-PREPARE-EXAMPLES "Examples") in the [PREPARE](sql-prepare.md "PREPARE") documentation. 

## Compatibility

The SQL standard includes an `EXECUTE` statement, but it is only for use in embedded SQL. This version of the `EXECUTE` statement also uses a somewhat different syntax. 

## See Also

[DEALLOCATE](sql-deallocate.md "DEALLOCATE"), [PREPARE](sql-prepare.md "PREPARE")

* * *

[Prev](sql-end.md "END") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-explain.md "EXPLAIN")  
---|---|---  
END | [Home](index.md "PostgreSQL 17.5 Documentation")|  EXPLAIN
