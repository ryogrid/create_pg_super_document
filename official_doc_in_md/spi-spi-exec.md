SPI_exec  
---  
[Prev](spi-spi-execute.md "SPI_execute") | [Up](spi-interface.md "45.1. Interface Functions")| 45.1. Interface Functions| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](spi-spi-execute-extended.md "SPI_execute_extended")  
  
* * *

## SPI_exec

SPI_exec — execute a read/write command

## Synopsis
    
    
    int SPI_exec(const char * _command_ , long _count_)
    

## Description

`SPI_exec` is the same as `SPI_execute`, with the latter's _`read_only`_ parameter always taken as `false`. 

## Arguments

`const char * _`command`_`
    

string containing command to execute 

`long _`count`_`
    

maximum number of rows to return, or `0` for no limit 

## Return Value

See `SPI_execute`. 

* * *

[Prev](spi-spi-execute.md "SPI_execute") | [Up](spi-interface.md "45.1. Interface Functions")|  [Next](spi-spi-execute-extended.md "SPI_execute_extended")  
---|---|---  
SPI_execute | [Home](index.md "PostgreSQL 17.5 Documentation")|  SPI_execute_extended
