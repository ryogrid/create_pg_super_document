SPI_scroll_cursor_fetch  
---  
[Prev](spi-spi-cursor-move.md "SPI_cursor_move") | [Up](spi-interface.md "45.1. Interface Functions")| 45.1. Interface Functions| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](spi-spi-scroll-cursor-move.md "SPI_scroll_cursor_move")  
  
* * *

## SPI_scroll_cursor_fetch

SPI_scroll_cursor_fetch — fetch some rows from a cursor

## Synopsis
    
    
    void SPI_scroll_cursor_fetch(Portal _portal_ , FetchDirection _direction_ ,
                                 long _count_)
    

## Description

`SPI_scroll_cursor_fetch` fetches some rows from a cursor. This is equivalent to the SQL command `FETCH`. 

## Arguments

`Portal _`portal`_`
    

portal containing the cursor 

`FetchDirection _`direction`_`
    

one of `FETCH_FORWARD`, `FETCH_BACKWARD`, `FETCH_ABSOLUTE` or `FETCH_RELATIVE`

`long _`count`_`
    

number of rows to fetch for `FETCH_FORWARD` or `FETCH_BACKWARD`; absolute row number to fetch for `FETCH_ABSOLUTE`; or relative row number to fetch for `FETCH_RELATIVE`

## Return Value

`SPI_processed` and `SPI_tuptable` are set as in `SPI_execute` if successful. 

## Notes

See the SQL [FETCH](sql-fetch.md "FETCH") command for details of the interpretation of the _`direction`_ and _`count`_ parameters. 

Direction values other than `FETCH_FORWARD` may fail if the cursor's plan was not created with the `CURSOR_OPT_SCROLL` option. 

* * *

[Prev](spi-spi-cursor-move.md "SPI_cursor_move") | [Up](spi-interface.md "45.1. Interface Functions")|  [Next](spi-spi-scroll-cursor-move.md "SPI_scroll_cursor_move")  
---|---|---  
SPI_cursor_move | [Home](index.md "PostgreSQL 17.5 Documentation")|  SPI_scroll_cursor_move
