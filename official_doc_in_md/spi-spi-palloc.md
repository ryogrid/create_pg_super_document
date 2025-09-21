SPI_palloc  
---  
[Prev](spi-memory.md "45.3. Memory Management") | [Up](spi-memory.md "45.3. Memory Management")| 45.3. Memory Management| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](spi-realloc.md "SPI_repalloc")  
  
* * *

## SPI_palloc

SPI_palloc — allocate memory in the upper executor context

## Synopsis
    
    
    void * SPI_palloc(Size _size_)
    

## Description

`SPI_palloc` allocates memory in the upper executor context. 

This function can only be used while connected to SPI. Otherwise, it throws an error. 

## Arguments

`Size _`size`_`
    

size in bytes of storage to allocate 

## Return Value

pointer to new storage space of the specified size 

* * *

[Prev](spi-memory.md "45.3. Memory Management") | [Up](spi-memory.md "45.3. Memory Management")|  [Next](spi-realloc.md "SPI_repalloc")  
---|---|---  
45.3. Memory Management | [Home](index.md "PostgreSQL 17.5 Documentation")|  SPI_repalloc
