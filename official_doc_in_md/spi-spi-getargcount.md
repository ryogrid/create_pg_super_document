SPI_getargcount  
---  
[Prev](spi-spi-prepare-params.md "SPI_prepare_params") | [Up](spi-interface.md "45.1. Interface Functions")| 45.1. Interface Functions| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](spi-spi-getargtypeid.md "SPI_getargtypeid")  
  
* * *

## SPI_getargcount

SPI_getargcount — return the number of arguments needed by a statement prepared by `SPI_prepare`

## Synopsis
    
    
    int SPI_getargcount(SPIPlanPtr _plan_)
    

## Description

`SPI_getargcount` returns the number of arguments needed to execute a statement prepared by `SPI_prepare`. 

## Arguments

`SPIPlanPtr _`plan`_`
    

prepared statement (returned by `SPI_prepare`) 

## Return Value

The count of expected arguments for the _`plan`_. If the _`plan`_ is `NULL` or invalid, `SPI_result` is set to `SPI_ERROR_ARGUMENT` and -1 is returned. 

* * *

[Prev](spi-spi-prepare-params.md "SPI_prepare_params") | [Up](spi-interface.md "45.1. Interface Functions")|  [Next](spi-spi-getargtypeid.md "SPI_getargtypeid")  
---|---|---  
SPI_prepare_params | [Home](index.md "PostgreSQL 17.5 Documentation")|  SPI_getargtypeid
