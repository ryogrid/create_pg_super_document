EXECUTE IMMEDIATE  
---  
[Prev](ecpg-sql-disconnect.md "DISCONNECT") | [Up](ecpg-sql-commands.md "34.14. Embedded SQL Commands")| 34.14. Embedded SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](ecpg-sql-get-descriptor.md "GET DESCRIPTOR")  
  
* * *

## EXECUTE IMMEDIATE

EXECUTE IMMEDIATE — dynamically prepare and execute a statement

## Synopsis
    
    
    EXECUTE IMMEDIATE _string_
    

## Description

`EXECUTE IMMEDIATE` immediately prepares and executes a dynamically specified SQL statement, without retrieving result rows. 

## Parameters

 _`string`_ #
    

A literal string or a host variable containing the SQL statement to be executed. 

## Notes

In typical usage, the _`string`_ is a host variable reference to a string containing a dynamically-constructed SQL statement. The case of a literal string is not very useful; you might as well just write the SQL statement directly, without the extra typing of `EXECUTE IMMEDIATE`. 

If you do use a literal string, keep in mind that any double quotes you might wish to include in the SQL statement must be written as octal escapes (`\042`) not the usual C idiom `\"`. This is because the string is inside an `EXEC SQL` section, so the ECPG lexer parses it according to SQL rules not C rules. Any embedded backslashes will later be handled according to C rules; but `\"` causes an immediate syntax error because it is seen as ending the literal. 

## Examples

Here is an example that executes an `INSERT` statement using `EXECUTE IMMEDIATE` and a host variable named `command`: 
    
    
    sprintf(command, "INSERT INTO test (name, amount, letter) VALUES ('db: ''r1''', 1, 'f')");
    EXEC SQL EXECUTE IMMEDIATE :command;
    

## Compatibility

`EXECUTE IMMEDIATE` is specified in the SQL standard. 

* * *

[Prev](ecpg-sql-disconnect.md "DISCONNECT") | [Up](ecpg-sql-commands.md "34.14. Embedded SQL Commands")|  [Next](ecpg-sql-get-descriptor.md "GET DESCRIPTOR")  
---|---|---  
DISCONNECT | [Home](index.md "PostgreSQL 17.5 Documentation")|  GET DESCRIPTOR
