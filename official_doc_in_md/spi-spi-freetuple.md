SPI_freetuple  
---  
[Prev](spi-spi-modifytuple.md "SPI_modifytuple") | [Up](spi-memory.md "45.3. Memory Management")| 45.3. Memory Management| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](spi-spi-freetupletable.md "SPI_freetuptable")  
  
* * *

## SPI_freetuple

SPI_freetuple — free a row allocated in the upper executor context

## Synopsis
    
    
    void SPI_freetuple(HeapTuple _row_)
    

## Description

`SPI_freetuple` frees a row previously allocated in the upper executor context. 

This function is no longer different from plain `heap_freetuple`. It's kept just for backward compatibility of existing code. 

## Arguments

`HeapTuple _`row`_`
    

row to free 

* * *

[Prev](spi-spi-modifytuple.md "SPI_modifytuple") | [Up](spi-memory.md "45.3. Memory Management")|  [Next](spi-spi-freetupletable.md "SPI_freetuptable")  
---|---|---  
SPI_modifytuple | [Home](index.md "PostgreSQL 17.5 Documentation")|  SPI_freetuptable
