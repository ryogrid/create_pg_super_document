SPI_pfree  
---  
[Prev](spi-realloc.md "SPI_repalloc") | [Up](spi-memory.md "45.3. Memory Management")| 45.3. Memory Management| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](spi-spi-copytuple.md "SPI_copytuple")  
  
* * *

## SPI_pfree

SPI_pfree — free memory in the upper executor context

## Synopsis
    
    
    void SPI_pfree(void * _pointer_)
    

## Description

`SPI_pfree` frees memory previously allocated using `SPI_palloc` or `SPI_repalloc`. 

This function is no longer different from plain `pfree`. It's kept just for backward compatibility of existing code. 

## Arguments

`void * _`pointer`_`
    

pointer to existing storage to free 

* * *

[Prev](spi-realloc.md "SPI_repalloc") | [Up](spi-memory.md "45.3. Memory Management")|  [Next](spi-spi-copytuple.md "SPI_copytuple")  
---|---|---  
SPI_repalloc | [Home](index.md "PostgreSQL 17.5 Documentation")|  SPI_copytuple
