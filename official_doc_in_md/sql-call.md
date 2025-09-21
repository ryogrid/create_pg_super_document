CALL  
---  
[Prev](sql-begin.md "BEGIN") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-checkpoint.md "CHECKPOINT")  
  
* * *

## CALL

CALL — invoke a procedure

## Synopsis
    
    
    CALL _name_ ( [ _argument_ ] [, ...] )
    

## Description

`CALL` executes a procedure. 

If the procedure has any output parameters, then a result row will be returned, containing the values of those parameters. 

## Parameters

 _`name`_
    

The name (optionally schema-qualified) of the procedure. 

_`argument`_
    

An argument expression for the procedure call. 

Arguments can include parameter names, using the syntax `_`name`_ => _`value`_`. This works the same as in ordinary function calls; see [Section 4.3](sql-syntax-calling-funcs.md "4.3. Calling Functions") for details. 

Arguments must be supplied for all procedure parameters that lack defaults, including `OUT` parameters. However, arguments matching `OUT` parameters are not evaluated, so it's customary to just write `NULL` for them. (Writing something else for an `OUT` parameter might cause compatibility problems with future PostgreSQL versions.) 

## Notes

The user must have `EXECUTE` privilege on the procedure in order to be allowed to invoke it. 

To call a function (not a procedure), use `SELECT` instead. 

If `CALL` is executed in a transaction block, then the called procedure cannot execute transaction control statements. Transaction control statements are only allowed if `CALL` is executed in its own transaction. 

PL/pgSQL handles output parameters in `CALL` commands differently; see [Section 41.6.3](plpgsql-control-structures.md#PLPGSQL-STATEMENTS-CALLING-PROCEDURE "41.6.3. Calling a Procedure"). 

## Examples
    
    
    CALL do_db_maintenance();
    

## Compatibility

`CALL` conforms to the SQL standard, except for the handling of output parameters. The standard says that users should write variables to receive the values of output parameters. 

## See Also

[CREATE PROCEDURE](sql-createprocedure.md "CREATE PROCEDURE")

* * *

[Prev](sql-begin.md "BEGIN") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-checkpoint.md "CHECKPOINT")  
---|---|---  
BEGIN | [Home](index.md "PostgreSQL 17.5 Documentation")|  CHECKPOINT
