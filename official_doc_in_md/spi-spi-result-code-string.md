SPI_result_code_string  
---  
[Prev](spi-spi-getnspname.md "SPI_getnspname") | [Up](spi-interface-support.md "45.2. Interface Support Functions")| 45.2. Interface Support Functions| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](spi-memory.md "45.3. Memory Management")  
  
* * *

## SPI_result_code_string

SPI_result_code_string — return error code as string

## Synopsis
    
    
    const char * SPI_result_code_string(int _code_);
    

## Description

`SPI_result_code_string` returns a string representation of the result code returned by various SPI functions or stored in `SPI_result`. 

## Arguments

`int _`code`_`
    

result code 

## Return Value

A string representation of the result code. 

* * *

[Prev](spi-spi-getnspname.md "SPI_getnspname") | [Up](spi-interface-support.md "45.2. Interface Support Functions")|  [Next](spi-memory.md "45.3. Memory Management")  
---|---|---  
SPI_getnspname | [Home](index.md "PostgreSQL 17.5 Documentation")|  45.3. Memory Management
