SPI_cursor_close  
---  
[Prev](spi-spi-scroll-cursor-move.md "SPI_scroll_cursor_move") | [Up](spi-interface.md "45.1. Interface Functions")| 45.1. Interface Functions| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](spi-spi-keepplan.md "SPI_keepplan")  
  
* * *

## SPI_cursor_close

SPI_cursor_close — close a cursor

## Synopsis
    
    
    void SPI_cursor_close(Portal _portal_)
    

## Description

`SPI_cursor_close` closes a previously created cursor and releases its portal storage. 

All open cursors are closed automatically at the end of a transaction. `SPI_cursor_close` need only be invoked if it is desirable to release resources sooner. 

## Arguments

`Portal _`portal`_`
    

portal containing the cursor 

* * *

[Prev](spi-spi-scroll-cursor-move.md "SPI_scroll_cursor_move") | [Up](spi-interface.md "45.1. Interface Functions")|  [Next](spi-spi-keepplan.md "SPI_keepplan")  
---|---|---  
SPI_scroll_cursor_move | [Home](index.md "PostgreSQL 17.5 Documentation")|  SPI_keepplan
