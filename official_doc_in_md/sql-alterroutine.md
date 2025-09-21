ALTER ROUTINE  
---  
[Prev](sql-alterrole.md "ALTER ROLE") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-alterrule.md "ALTER RULE")  
  
* * *

## ALTER ROUTINE

ALTER ROUTINE — change the definition of a routine

## Synopsis
    
    
    ALTER ROUTINE _name_ [ ( [ [ _argmode_ ] [ _argname_ ] _argtype_ [, ...] ] ) ]
        _action_ [ ... ] [ RESTRICT ]
    ALTER ROUTINE _name_ [ ( [ [ _argmode_ ] [ _argname_ ] _argtype_ [, ...] ] ) ]
        RENAME TO _new_name_
    ALTER ROUTINE _name_ [ ( [ [ _argmode_ ] [ _argname_ ] _argtype_ [, ...] ] ) ]
        OWNER TO { _new_owner_ | CURRENT_ROLE | CURRENT_USER | SESSION_USER }
    ALTER ROUTINE _name_ [ ( [ [ _argmode_ ] [ _argname_ ] _argtype_ [, ...] ] ) ]
        SET SCHEMA _new_schema_
    ALTER ROUTINE _name_ [ ( [ [ _argmode_ ] [ _argname_ ] _argtype_ [, ...] ] ) ]
        [ NO ] DEPENDS ON EXTENSION _extension_name_
    
    where _action_ is one of:
    
        IMMUTABLE | STABLE | VOLATILE
        [ NOT ] LEAKPROOF
        [ EXTERNAL ] SECURITY INVOKER | [ EXTERNAL ] SECURITY DEFINER
        PARALLEL { UNSAFE | RESTRICTED | SAFE }
        COST _execution_cost_
        ROWS _result_rows_
        SET _configuration_parameter_ { TO | = } { _value_ | DEFAULT }
        SET _configuration_parameter_ FROM CURRENT
        RESET _configuration_parameter_
        RESET ALL
    

## Description

`ALTER ROUTINE` changes the definition of a routine, which can be an aggregate function, a normal function, or a procedure. See under [ALTER AGGREGATE](sql-alteraggregate.md "ALTER AGGREGATE"), [ALTER FUNCTION](sql-alterfunction.md "ALTER FUNCTION"), and [ALTER PROCEDURE](sql-alterprocedure.md "ALTER PROCEDURE") for the description of the parameters, more examples, and further details. 

## Examples

To rename the routine `foo` for type `integer` to `foobar`: 
    
    
    ALTER ROUTINE foo(integer) RENAME TO foobar;
    

This command will work independent of whether `foo` is an aggregate, function, or procedure. 

## Compatibility

This statement is partially compatible with the `ALTER ROUTINE` statement in the SQL standard. See under [ALTER FUNCTION](sql-alterfunction.md "ALTER FUNCTION") and [ALTER PROCEDURE](sql-alterprocedure.md "ALTER PROCEDURE") for more details. Allowing routine names to refer to aggregate functions is a PostgreSQL extension. 

## See Also

[ALTER AGGREGATE](sql-alteraggregate.md "ALTER AGGREGATE"), [ALTER FUNCTION](sql-alterfunction.md "ALTER FUNCTION"), [ALTER PROCEDURE](sql-alterprocedure.md "ALTER PROCEDURE"), [DROP ROUTINE](sql-droproutine.md "DROP ROUTINE")

Note that there is no `CREATE ROUTINE` command. 

* * *

[Prev](sql-alterrole.md "ALTER ROLE") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-alterrule.md "ALTER RULE")  
---|---|---  
ALTER ROLE | [Home](index.md "PostgreSQL 17.5 Documentation")|  ALTER RULE
