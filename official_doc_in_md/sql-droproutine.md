DROP ROUTINE  
---  
[Prev](sql-droprole.md "DROP ROLE") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-droprule.md "DROP RULE")  
  
* * *

## DROP ROUTINE

DROP ROUTINE — remove a routine

## Synopsis
    
    
    DROP ROUTINE [ IF EXISTS ] _name_ [ ( [ [ _argmode_ ] [ _argname_ ] _argtype_ [, ...] ] ) ] [, ...]
        [ CASCADE | RESTRICT ]
    

## Description

`DROP ROUTINE` removes the definition of one or more existing routines. The term “routine” includes aggregate functions, normal functions, and procedures. See under [DROP AGGREGATE](sql-dropaggregate.md "DROP AGGREGATE"), [DROP FUNCTION](sql-dropfunction.md "DROP FUNCTION"), and [DROP PROCEDURE](sql-dropprocedure.md "DROP PROCEDURE") for the description of the parameters, more examples, and further details. 

## Notes

The lookup rules used by `DROP ROUTINE` are fundamentally the same as for `DROP PROCEDURE`; in particular, `DROP ROUTINE` shares that command's behavior of considering an argument list that has no _`argmode`_ markers to be possibly using the SQL standard's definition that `OUT` arguments are included in the list. (`DROP AGGREGATE` and `DROP FUNCTION` do not do that.) 

In some cases where the same name is shared by routines of different kinds, it is possible for `DROP ROUTINE` to fail with an ambiguity error when a more specific command (`DROP FUNCTION`, etc.) would work. Specifying the argument type list more carefully will also resolve such problems. 

These lookup rules are also used by other commands that act on existing routines, such as `ALTER ROUTINE` and `COMMENT ON ROUTINE`. 

## Examples

To drop the routine `foo` for type `integer`: 
    
    
    DROP ROUTINE foo(integer);
    

This command will work independent of whether `foo` is an aggregate, function, or procedure. 

## Compatibility

This command conforms to the SQL standard, with these PostgreSQL extensions: 

  * The standard only allows one routine to be dropped per command.

  * The `IF EXISTS` option is an extension.

  * The ability to specify argument modes and names is an extension, and the lookup rules differ when modes are given.

  * User-definable aggregate functions are an extension.




## See Also

[DROP AGGREGATE](sql-dropaggregate.md "DROP AGGREGATE"), [DROP FUNCTION](sql-dropfunction.md "DROP FUNCTION"), [DROP PROCEDURE](sql-dropprocedure.md "DROP PROCEDURE"), [ALTER ROUTINE](sql-alterroutine.md "ALTER ROUTINE")

Note that there is no `CREATE ROUTINE` command. 

* * *

[Prev](sql-droprole.md "DROP ROLE") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-droprule.md "DROP RULE")  
---|---|---  
DROP ROLE | [Home](index.md "PostgreSQL 17.5 Documentation")|  DROP RULE
