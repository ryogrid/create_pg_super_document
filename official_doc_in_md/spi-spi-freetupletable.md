SPI_freetuptable  
---  
[Prev](spi-spi-freetuple.md "SPI_freetuple") | [Up](spi-memory.md "45.3. Memory Management")| 45.3. Memory Management| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](spi-spi-freeplan.md "SPI_freeplan")  
  
* * *

## SPI_freetuptable

SPI_freetuptable — free a row set created by `SPI_execute` or a similar function

## Synopsis
    
    
    void SPI_freetuptable(SPITupleTable * _tuptable_)
    

## Description

`SPI_freetuptable` frees a row set created by a prior SPI command execution function, such as `SPI_execute`. Therefore, this function is often called with the global variable `SPI_tuptable` as argument. 

This function is useful if an SPI-using C function needs to execute multiple commands and does not want to keep the results of earlier commands around until it ends. Note that any unfreed row sets will be freed anyway at `SPI_finish`. Also, if a subtransaction is started and then aborted within execution of an SPI-using C function, SPI automatically frees any row sets created while the subtransaction was running. 

Beginning in PostgreSQL 9.3, `SPI_freetuptable` contains guard logic to protect against duplicate deletion requests for the same row set. In previous releases, duplicate deletions would lead to crashes. 

## Arguments

`SPITupleTable * _`tuptable`_`
    

pointer to row set to free, or NULL to do nothing 

* * *

[Prev](spi-spi-freetuple.md "SPI_freetuple") | [Up](spi-memory.md "45.3. Memory Management")|  [Next](spi-spi-freeplan.md "SPI_freeplan")  
---|---|---  
SPI_freetuple | [Home](index.md "PostgreSQL 17.5 Documentation")|  SPI_freeplan
