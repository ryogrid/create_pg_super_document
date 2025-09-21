SPI_execute_plan_with_paramlist  
---  
[Prev](spi-spi-execute-plan-extended.md "SPI_execute_plan_extended") | [Up](spi-interface.md "45.1. Interface Functions")| 45.1. Interface Functions| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](spi-spi-execp.md "SPI_execp")  
  
* * *

## SPI_execute_plan_with_paramlist

SPI_execute_plan_with_paramlist — execute a statement prepared by `SPI_prepare`

## Synopsis
    
    
    int SPI_execute_plan_with_paramlist(SPIPlanPtr _plan_ ,
                                        ParamListInfo _params_ ,
                                        bool _read_only_ ,
                                        long _count_)
    

## Description

`SPI_execute_plan_with_paramlist` executes a statement prepared by `SPI_prepare`. This function is equivalent to `SPI_execute_plan` except that information about the parameter values to be passed to the query is presented differently. The `ParamListInfo` representation can be convenient for passing down values that are already available in that format. It also supports use of dynamic parameter sets via hook functions specified in `ParamListInfo`. 

This function is now deprecated in favor of `SPI_execute_plan_extended`. 

## Arguments

`SPIPlanPtr _`plan`_`
    

prepared statement (returned by `SPI_prepare`) 

`ParamListInfo _`params`_`
    

data structure containing parameter types and values; NULL if none 

`bool _`read_only`_`
    

`true` for read-only execution

`long _`count`_`
    

maximum number of rows to return, or `0` for no limit 

## Return Value

The return value is the same as for `SPI_execute_plan`. 

`SPI_processed` and `SPI_tuptable` are set as in `SPI_execute_plan` if successful. 

* * *

[Prev](spi-spi-execute-plan-extended.md "SPI_execute_plan_extended") | [Up](spi-interface.md "45.1. Interface Functions")|  [Next](spi-spi-execp.md "SPI_execp")  
---|---|---  
SPI_execute_plan_extended | [Home](index.md "PostgreSQL 17.5 Documentation")|  SPI_execp
