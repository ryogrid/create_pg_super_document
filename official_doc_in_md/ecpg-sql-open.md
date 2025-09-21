OPEN  
---  
[Prev](ecpg-sql-get-descriptor.md "GET DESCRIPTOR") | [Up](ecpg-sql-commands.md "34.14. Embedded SQL Commands")| 34.14. Embedded SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](ecpg-sql-prepare.md "PREPARE")  
  
* * *

## OPEN

OPEN — open a dynamic cursor

## Synopsis
    
    
    OPEN _cursor_name_
    OPEN _cursor_name_ USING _value_ [, ... ]
    OPEN _cursor_name_ USING SQL DESCRIPTOR _descriptor_name_
    

## Description

`OPEN` opens a cursor and optionally binds actual values to the placeholders in the cursor's declaration. The cursor must previously have been declared with the `DECLARE` command. The execution of `OPEN` causes the query to start executing on the server. 

## Parameters

 _`cursor_name`_ #
    

The name of the cursor to be opened. This can be an SQL identifier or a host variable. 

_`value`_ #
    

A value to be bound to a placeholder in the cursor. This can be an SQL constant, a host variable, or a host variable with indicator. 

_`descriptor_name`_ #
    

The name of a descriptor containing values to be bound to the placeholders in the cursor. This can be an SQL identifier or a host variable. 

## Examples
    
    
    EXEC SQL OPEN a;
    EXEC SQL OPEN d USING 1, 'test';
    EXEC SQL OPEN c1 USING SQL DESCRIPTOR mydesc;
    EXEC SQL OPEN :curname1;
    

## Compatibility

`OPEN` is specified in the SQL standard. 

## See Also

[DECLARE](ecpg-sql-declare.md "DECLARE"), [CLOSE](sql-close.md "CLOSE")

* * *

[Prev](ecpg-sql-get-descriptor.md "GET DESCRIPTOR") | [Up](ecpg-sql-commands.md "34.14. Embedded SQL Commands")|  [Next](ecpg-sql-prepare.md "PREPARE")  
---|---|---  
GET DESCRIPTOR | [Home](index.md "PostgreSQL 17.5 Documentation")|  PREPARE
