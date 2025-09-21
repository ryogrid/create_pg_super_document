VAR  
---  
[Prev](ecpg-sql-type.md "TYPE") | [Up](ecpg-sql-commands.md "34.14. Embedded SQL Commands")| 34.14. Embedded SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](ecpg-sql-whenever.md "WHENEVER")  
  
* * *

## VAR

VAR — define a variable

## Synopsis
    
    
    VAR _varname_ IS _ctype_
    

## Description

The `VAR` command assigns a new C data type to a host variable. The host variable must be previously declared in a declare section. 

## Parameters

 _`varname`_ #
    

A C variable name. 

_`ctype`_ #
    

A C type specification. 

## Examples
    
    
    Exec sql begin declare section;
    short a;
    exec sql end declare section;
    EXEC SQL VAR a IS int;
    

## Compatibility

The `VAR` command is a PostgreSQL extension. 

* * *

[Prev](ecpg-sql-type.md "TYPE") | [Up](ecpg-sql-commands.md "34.14. Embedded SQL Commands")|  [Next](ecpg-sql-whenever.md "WHENEVER")  
---|---|---  
TYPE | [Home](index.md "PostgreSQL 17.5 Documentation")|  WHENEVER
