SET DESCRIPTOR  
---  
[Prev](ecpg-sql-set-connection.md "SET CONNECTION") | [Up](ecpg-sql-commands.md "34.14. Embedded SQL Commands")| 34.14. Embedded SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](ecpg-sql-type.md "TYPE")  
  
* * *

## SET DESCRIPTOR

SET DESCRIPTOR — set information in an SQL descriptor area

## Synopsis
    
    
    SET DESCRIPTOR _descriptor_name_ _descriptor_header_item_ = _value_ [, ... ]
    SET DESCRIPTOR _descriptor_name_ VALUE _number_ _descriptor_item_ = _value_ [, ...]
    

## Description

`SET DESCRIPTOR` populates an SQL descriptor area with values. The descriptor area is then typically used to bind parameters in a prepared query execution. 

This command has two forms: The first form applies to the descriptor “header”, which is independent of a particular datum. The second form assigns values to particular datums, identified by number. 

## Parameters

 _`descriptor_name`_ #
    

A descriptor name. 

_`descriptor_header_item`_ #
    

A token identifying which header information item to set. Only `COUNT`, to set the number of descriptor items, is currently supported. 

_`number`_ #
    

The number of the descriptor item to set. The count starts at 1\. 

_`descriptor_item`_ #
    

A token identifying which item of information to set in the descriptor. See [Section 34.7.1](ecpg-descriptors.md#ECPG-NAMED-DESCRIPTORS "34.7.1. Named SQL Descriptor Areas") for a list of supported items. 

_`value`_ #
    

A value to store into the descriptor item. This can be an SQL constant or a host variable. 

## Examples
    
    
    EXEC SQL SET DESCRIPTOR indesc COUNT = 1;
    EXEC SQL SET DESCRIPTOR indesc VALUE 1 DATA = 2;
    EXEC SQL SET DESCRIPTOR indesc VALUE 1 DATA = :val1;
    EXEC SQL SET DESCRIPTOR indesc VALUE 2 INDICATOR = :val1, DATA = 'some string';
    EXEC SQL SET DESCRIPTOR indesc VALUE 2 INDICATOR = :val2null, DATA = :val2;
    

## Compatibility

`SET DESCRIPTOR` is specified in the SQL standard. 

## See Also

[ALLOCATE DESCRIPTOR](ecpg-sql-allocate-descriptor.md "ALLOCATE DESCRIPTOR"), [GET DESCRIPTOR](ecpg-sql-get-descriptor.md "GET DESCRIPTOR")

* * *

[Prev](ecpg-sql-set-connection.md "SET CONNECTION") | [Up](ecpg-sql-commands.md "34.14. Embedded SQL Commands")|  [Next](ecpg-sql-type.md "TYPE")  
---|---|---  
SET CONNECTION | [Home](index.md "PostgreSQL 17.5 Documentation")|  TYPE
