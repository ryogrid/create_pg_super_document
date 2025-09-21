SPI_cursor_move  
---  
[Prev](spi-spi-cursor-fetch.md "SPI_cursor_fetch") | [Up](spi-interface.md "45.1. Interface Functions")| 45.1. Interface Functions| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](spi-spi-scroll-cursor-fetch.md "SPI_scroll_cursor_fetch")  
  
* * *

## SPI_cursor_move

SPI_cursor_move — move a cursor

## Synopsis
    
    
    void SPI_cursor_move(Portal _portal_ , bool _forward_ , long _count_)
    

## Description

`SPI_cursor_move` skips over some number of rows in a cursor. This is equivalent to a subset of the SQL command `MOVE` (see `SPI_scroll_cursor_move` for more functionality). 

## Arguments

`Portal _`portal`_`
    

portal containing the cursor 

`bool _`forward`_`
    

true for move forward, false for move backward 

`long _`count`_`
    

maximum number of rows to move 

## Notes

Moving backward may fail if the cursor's plan was not created with the `CURSOR_OPT_SCROLL` option. 

* * *

[Prev](spi-spi-cursor-fetch.md "SPI_cursor_fetch") | [Up](spi-interface.md "45.1. Interface Functions")|  [Next](spi-spi-scroll-cursor-fetch.md "SPI_scroll_cursor_fetch")  
---|---|---  
SPI_cursor_fetch | [Home](index.md "PostgreSQL 17.5 Documentation")|  SPI_scroll_cursor_fetch
