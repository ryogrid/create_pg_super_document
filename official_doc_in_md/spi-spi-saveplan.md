SPI_saveplan  
---  
[Prev](spi-spi-keepplan.md "SPI_keepplan") | [Up](spi-interface.md "45.1. Interface Functions")| 45.1. Interface Functions| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](spi-spi-register-relation.md "SPI_register_relation")  
  
* * *

## SPI_saveplan

SPI_saveplan — save a prepared statement

## Synopsis
    
    
    SPIPlanPtr SPI_saveplan(SPIPlanPtr _plan_)
    

## Description

`SPI_saveplan` copies a passed statement (prepared by `SPI_prepare`) into memory that will not be freed by `SPI_finish` nor by the transaction manager, and returns a pointer to the copied statement. This gives you the ability to reuse prepared statements in the subsequent invocations of your C function in the current session. 

## Arguments

`SPIPlanPtr _`plan`_`
    

the prepared statement to be saved 

## Return Value

Pointer to the copied statement; or `NULL` if unsuccessful. On error, `SPI_result` is set thus: 

`SPI_ERROR_ARGUMENT`
    

if _`plan`_ is `NULL` or invalid 

`SPI_ERROR_UNCONNECTED`
    

if called from an unconnected C function 

## Notes

The originally passed-in statement is not freed, so you might wish to do `SPI_freeplan` on it to avoid leaking memory until `SPI_finish`. 

In most cases, `SPI_keepplan` is preferred to this function, since it accomplishes largely the same result without needing to physically copy the prepared statement's data structures. 

* * *

[Prev](spi-spi-keepplan.md "SPI_keepplan") | [Up](spi-interface.md "45.1. Interface Functions")|  [Next](spi-spi-register-relation.md "SPI_register_relation")  
---|---|---  
SPI_keepplan | [Home](index.md "PostgreSQL 17.5 Documentation")|  SPI_register_relation
