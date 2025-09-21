SPI_finish  
---  
[Prev](spi-spi-connect.md "SPI_connect") | [Up](spi-interface.md "45.1. Interface Functions")| 45.1. Interface Functions| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](spi-spi-execute.md "SPI_execute")  
  
* * *

## SPI_finish

SPI_finish — disconnect a C function from the SPI manager

## Synopsis
    
    
    int SPI_finish(void)
    

## Description

`SPI_finish` closes an existing connection to the SPI manager. You must call this function after completing the SPI operations needed during your C function's current invocation. You do not need to worry about making this happen, however, if you abort the transaction via `elog(ERROR)`. In that case SPI will clean itself up automatically. 

## Return Value

`SPI_OK_FINISH`
    

if properly disconnected 

`SPI_ERROR_UNCONNECTED`
    

if called from an unconnected C function 

* * *

[Prev](spi-spi-connect.md "SPI_connect") | [Up](spi-interface.md "45.1. Interface Functions")|  [Next](spi-spi-execute.md "SPI_execute")  
---|---|---  
SPI_connect | [Home](index.md "PostgreSQL 17.5 Documentation")|  SPI_execute
