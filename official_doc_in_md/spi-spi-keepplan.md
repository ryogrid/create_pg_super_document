SPI_keepplan  
---  
[Prev](spi-spi-cursor-close.md "SPI_cursor_close") | [Up](spi-interface.md "45.1. Interface Functions")| 45.1. Interface Functions| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](spi-spi-saveplan.md "SPI_saveplan")  
  
* * *

## SPI_keepplan

SPI_keepplan — save a prepared statement

## Synopsis
    
    
    int SPI_keepplan(SPIPlanPtr _plan_)
    

## Description

`SPI_keepplan` saves a passed statement (prepared by `SPI_prepare`) so that it will not be freed by `SPI_finish` nor by the transaction manager. This gives you the ability to reuse prepared statements in the subsequent invocations of your C function in the current session. 

## Arguments

`SPIPlanPtr _`plan`_`
    

the prepared statement to be saved 

## Return Value

0 on success; `SPI_ERROR_ARGUMENT` if _`plan`_ is `NULL` or invalid 

## Notes

The passed-in statement is relocated to permanent storage by means of pointer adjustment (no data copying is required). If you later wish to delete it, use `SPI_freeplan` on it. 

* * *

[Prev](spi-spi-cursor-close.md "SPI_cursor_close") | [Up](spi-interface.md "45.1. Interface Functions")|  [Next](spi-spi-saveplan.md "SPI_saveplan")  
---|---|---  
SPI_cursor_close | [Home](index.md "PostgreSQL 17.5 Documentation")|  SPI_saveplan
