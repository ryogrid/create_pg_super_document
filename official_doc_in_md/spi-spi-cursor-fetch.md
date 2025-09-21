SPI_cursor_fetch  
---  
[Prev](spi-spi-cursor-find.md "SPI_cursor_find") | [Up](spi-interface.md "45.1. Interface Functions")| 45.1. Interface Functions| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](spi-spi-cursor-move.md "SPI_cursor_move")  
  
* * *

## SPI_cursor_fetch

SPI_cursor_fetch — fetch some rows from a cursor

## Synopsis
    
    
    void SPI_cursor_fetch(Portal _portal_ , bool _forward_ , long _count_)
    

## Description

`SPI_cursor_fetch` fetches some rows from a cursor. This is equivalent to a subset of the SQL command `FETCH` (see `SPI_scroll_cursor_fetch` for more functionality). 

## Arguments

`Portal _`portal`_`
    

portal containing the cursor 

`bool _`forward`_`
    

true for fetch forward, false for fetch backward 

`long _`count`_`
    

maximum number of rows to fetch 

## Return Value

`SPI_processed` and `SPI_tuptable` are set as in `SPI_execute` if successful. 

## Notes

Fetching backward may fail if the cursor's plan was not created with the `CURSOR_OPT_SCROLL` option. 

* * *

[Prev](spi-spi-cursor-find.md "SPI_cursor_find") | [Up](spi-interface.md "45.1. Interface Functions")|  [Next](spi-spi-cursor-move.md "SPI_cursor_move")  
---|---|---  
SPI_cursor_find | [Home](index.md "PostgreSQL 17.5 Documentation")|  SPI_cursor_move
