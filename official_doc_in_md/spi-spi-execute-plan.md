SPI_execute_plan  
---  
[Prev](spi-spi-is-cursor-plan.md "SPI_is_cursor_plan") | [Up](spi-interface.md "45.1. Interface Functions")| 45.1. Interface Functions| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](spi-spi-execute-plan-extended.md "SPI_execute_plan_extended")  
  
* * *

## SPI_execute_plan

SPI_execute_plan — execute a statement prepared by `SPI_prepare`

## Synopsis
    
    
    int SPI_execute_plan(SPIPlanPtr _plan_ , Datum * _values_ , const char * _nulls_ ,
                         bool _read_only_ , long _count_)
    

## Description

`SPI_execute_plan` executes a statement prepared by `SPI_prepare` or one of its siblings. _`read_only`_ and _`count`_ have the same interpretation as in `SPI_execute`. 

## Arguments

`SPIPlanPtr _`plan`_`
    

prepared statement (returned by `SPI_prepare`) 

`Datum * _`values`_`
    

An array of actual parameter values. Must have same length as the statement's number of arguments. 

`const char * _`nulls`_`
    

An array describing which parameters are null. Must have same length as the statement's number of arguments. 

If _`nulls`_ is `NULL` then `SPI_execute_plan` assumes that no parameters are null. Otherwise, each entry of the _`nulls`_ array should be `' '` if the corresponding parameter value is non-null, or `'n'` if the corresponding parameter value is null. (In the latter case, the actual value in the corresponding _`values`_ entry doesn't matter.) Note that _`nulls`_ is not a text string, just an array: it does not need a `'\0'` terminator. 

`bool _`read_only`_`
    

`true` for read-only execution

`long _`count`_`
    

maximum number of rows to return, or `0` for no limit 

## Return Value

The return value is the same as for `SPI_execute`, with the following additional possible error (negative) results: 

`SPI_ERROR_ARGUMENT`
    

if _`plan`_ is `NULL` or invalid, or _`count`_ is less than 0 

`SPI_ERROR_PARAM`
    

if _`values`_ is `NULL` and _`plan`_ was prepared with some parameters 

`SPI_processed` and `SPI_tuptable` are set as in `SPI_execute` if successful. 

* * *

[Prev](spi-spi-is-cursor-plan.md "SPI_is_cursor_plan") | [Up](spi-interface.md "45.1. Interface Functions")|  [Next](spi-spi-execute-plan-extended.md "SPI_execute_plan_extended")  
---|---|---  
SPI_is_cursor_plan | [Home](index.md "PostgreSQL 17.5 Documentation")|  SPI_execute_plan_extended
