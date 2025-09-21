SPI_getargtypeid  
---  
[Prev](spi-spi-getargcount.md "SPI_getargcount") | [Up](spi-interface.md "45.1. Interface Functions")| 45.1. Interface Functions| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](spi-spi-is-cursor-plan.md "SPI_is_cursor_plan")  
  
* * *

## SPI_getargtypeid

SPI_getargtypeid — return the data type OID for an argument of a statement prepared by `SPI_prepare`

## Synopsis
    
    
    Oid SPI_getargtypeid(SPIPlanPtr _plan_ , int _argIndex_)
    

## Description

`SPI_getargtypeid` returns the OID representing the type for the _`argIndex`_ 'th argument of a statement prepared by `SPI_prepare`. First argument is at index zero. 

## Arguments

`SPIPlanPtr _`plan`_`
    

prepared statement (returned by `SPI_prepare`) 

`int _`argIndex`_`
    

zero based index of the argument 

## Return Value

The type OID of the argument at the given index. If the _`plan`_ is `NULL` or invalid, or _`argIndex`_ is less than 0 or not less than the number of arguments declared for the _`plan`_ , `SPI_result` is set to `SPI_ERROR_ARGUMENT` and `InvalidOid` is returned. 

* * *

[Prev](spi-spi-getargcount.md "SPI_getargcount") | [Up](spi-interface.md "45.1. Interface Functions")|  [Next](spi-spi-is-cursor-plan.md "SPI_is_cursor_plan")  
---|---|---  
SPI_getargcount | [Home](index.md "PostgreSQL 17.5 Documentation")|  SPI_is_cursor_plan
