SPI_repalloc  
---  
[Prev](spi-spi-palloc.md "SPI_palloc") | [Up](spi-memory.md "45.3. Memory Management")| 45.3. Memory Management| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](spi-spi-pfree.md "SPI_pfree")  
  
* * *

## SPI_repalloc

SPI_repalloc — reallocate memory in the upper executor context

## Synopsis
    
    
    void * SPI_repalloc(void * _pointer_ , Size _size_)
    

## Description

`SPI_repalloc` changes the size of a memory segment previously allocated using `SPI_palloc`. 

This function is no longer different from plain `repalloc`. It's kept just for backward compatibility of existing code. 

## Arguments

`void * _`pointer`_`
    

pointer to existing storage to change 

`Size _`size`_`
    

size in bytes of storage to allocate 

## Return Value

pointer to new storage space of specified size with the contents copied from the existing area 

* * *

[Prev](spi-spi-palloc.md "SPI_palloc") | [Up](spi-memory.md "45.3. Memory Management")|  [Next](spi-spi-pfree.md "SPI_pfree")  
---|---|---  
SPI_palloc | [Home](index.md "PostgreSQL 17.5 Documentation")|  SPI_pfree
