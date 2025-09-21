SPI_execp  
---  
[Prev](spi-spi-execute-plan-with-paramlist.md "SPI_execute_plan_with_paramlist") | [Up](spi-interface.md "45.1. Interface Functions")| 45.1. Interface Functions| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](spi-spi-cursor-open.md "SPI_cursor_open")  
  
* * *

## SPI_execp

SPI_execp — execute a statement in read/write mode

## Synopsis
    
    
    int SPI_execp(SPIPlanPtr _plan_ , Datum * _values_ , const char * _nulls_ , long _count_)
    

## Description

`SPI_execp` is the same as `SPI_execute_plan`, with the latter's _`read_only`_ parameter always taken as `false`. 

## Arguments

`SPIPlanPtr _`plan`_`
    

prepared statement (returned by `SPI_prepare`) 

`Datum * _`values`_`
    

An array of actual parameter values. Must have same length as the statement's number of arguments. 

`const char * _`nulls`_`
    

An array describing which parameters are null. Must have same length as the statement's number of arguments. 

If _`nulls`_ is `NULL` then `SPI_execp` assumes that no parameters are null. Otherwise, each entry of the _`nulls`_ array should be `' '` if the corresponding parameter value is non-null, or `'n'` if the corresponding parameter value is null. (In the latter case, the actual value in the corresponding _`values`_ entry doesn't matter.) Note that _`nulls`_ is not a text string, just an array: it does not need a `'\0'` terminator. 

`long _`count`_`
    

maximum number of rows to return, or `0` for no limit 

## Return Value

See `SPI_execute_plan`. 

`SPI_processed` and `SPI_tuptable` are set as in `SPI_execute` if successful. 

* * *

[Prev](spi-spi-execute-plan-with-paramlist.md "SPI_execute_plan_with_paramlist") | [Up](spi-interface.md "45.1. Interface Functions")|  [Next](spi-spi-cursor-open.md "SPI_cursor_open")  
---|---|---  
SPI_execute_plan_with_paramlist | [Home](index.md "PostgreSQL 17.5 Documentation")|  SPI_cursor_open
