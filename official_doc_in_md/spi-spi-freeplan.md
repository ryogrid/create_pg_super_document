SPI_freeplan  
---  
[Prev](spi-spi-freetupletable.md "SPI_freetuptable") | [Up](spi-memory.md "45.3. Memory Management")| 45.3. Memory Management| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](spi-transaction.md "45.4. Transaction Management")  
  
* * *

## SPI_freeplan

SPI_freeplan — free a previously saved prepared statement

## Synopsis
    
    
    int SPI_freeplan(SPIPlanPtr _plan_)
    

## Description

`SPI_freeplan` releases a prepared statement previously returned by `SPI_prepare` or saved by `SPI_keepplan` or `SPI_saveplan`. 

## Arguments

`SPIPlanPtr _`plan`_`
    

pointer to statement to free 

## Return Value

0 on success; `SPI_ERROR_ARGUMENT` if _`plan`_ is `NULL` or invalid 

* * *

[Prev](spi-spi-freetupletable.md "SPI_freetuptable") | [Up](spi-memory.md "45.3. Memory Management")|  [Next](spi-transaction.md "45.4. Transaction Management")  
---|---|---  
SPI_freetuptable | [Home](index.md "PostgreSQL 17.5 Documentation")|  45.4. Transaction Management
