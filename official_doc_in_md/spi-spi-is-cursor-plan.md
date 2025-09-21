SPI_is_cursor_plan  
---  
[Prev](spi-spi-getargtypeid.md "SPI_getargtypeid") | [Up](spi-interface.md "45.1. Interface Functions")| 45.1. Interface Functions| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](spi-spi-execute-plan.md "SPI_execute_plan")  
  
* * *

## SPI_is_cursor_plan

SPI_is_cursor_plan — return `true` if a statement prepared by `SPI_prepare` can be used with `SPI_cursor_open`

## Synopsis
    
    
    bool SPI_is_cursor_plan(SPIPlanPtr _plan_)
    

## Description

`SPI_is_cursor_plan` returns `true` if a statement prepared by `SPI_prepare` can be passed as an argument to `SPI_cursor_open`, or `false` if that is not the case. The criteria are that the _`plan`_ represents one single command and that this command returns tuples to the caller; for example, `SELECT` is allowed unless it contains an `INTO` clause, and `UPDATE` is allowed only if it contains a `RETURNING` clause. 

## Arguments

`SPIPlanPtr _`plan`_`
    

prepared statement (returned by `SPI_prepare`) 

## Return Value

`true` or `false` to indicate if the _`plan`_ can produce a cursor or not, with `SPI_result` set to zero. If it is not possible to determine the answer (for example, if the _`plan`_ is `NULL` or invalid, or if called when not connected to SPI), then `SPI_result` is set to a suitable error code and `false` is returned. 

* * *

[Prev](spi-spi-getargtypeid.md "SPI_getargtypeid") | [Up](spi-interface.md "45.1. Interface Functions")|  [Next](spi-spi-execute-plan.md "SPI_execute_plan")  
---|---|---  
SPI_getargtypeid | [Home](index.md "PostgreSQL 17.5 Documentation")|  SPI_execute_plan
