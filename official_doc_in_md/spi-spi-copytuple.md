SPI_copytuple  
---  
[Prev](spi-spi-pfree.md "SPI_pfree") | [Up](spi-memory.md "45.3. Memory Management")| 45.3. Memory Management| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](spi-spi-returntuple.md "SPI_returntuple")  
  
* * *

## SPI_copytuple

SPI_copytuple — make a copy of a row in the upper executor context

## Synopsis
    
    
    HeapTuple SPI_copytuple(HeapTuple _row_)
    

## Description

`SPI_copytuple` makes a copy of a row in the upper executor context. This is normally used to return a modified row from a trigger. In a function declared to return a composite type, use `SPI_returntuple` instead. 

This function can only be used while connected to SPI. Otherwise, it returns NULL and sets `SPI_result` to `SPI_ERROR_UNCONNECTED`. 

## Arguments

`HeapTuple _`row`_`
    

row to be copied 

## Return Value

the copied row, or `NULL` on error (see `SPI_result` for an error indication) 

* * *

[Prev](spi-spi-pfree.md "SPI_pfree") | [Up](spi-memory.md "45.3. Memory Management")|  [Next](spi-spi-returntuple.md "SPI_returntuple")  
---|---|---  
SPI_pfree | [Home](index.md "PostgreSQL 17.5 Documentation")|  SPI_returntuple
