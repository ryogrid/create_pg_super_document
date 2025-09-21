SPI_scroll_cursor_move  
---  
[Prev](spi-spi-scroll-cursor-fetch.md "SPI_scroll_cursor_fetch") | [Up](spi-interface.md "45.1. Interface Functions")| 45.1. Interface Functions| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](spi-spi-cursor-close.md "SPI_cursor_close")  
  
* * *

## SPI_scroll_cursor_move

SPI_scroll_cursor_move — move a cursor

## Synopsis
    
    
    void SPI_scroll_cursor_move(Portal _portal_ , FetchDirection _direction_ ,
                                long _count_)
    

## Description

`SPI_scroll_cursor_move` skips over some number of rows in a cursor. This is equivalent to the SQL command `MOVE`. 

## Arguments

`Portal _`portal`_`
    

portal containing the cursor 

`FetchDirection _`direction`_`
    

one of `FETCH_FORWARD`, `FETCH_BACKWARD`, `FETCH_ABSOLUTE` or `FETCH_RELATIVE`

`long _`count`_`
    

number of rows to move for `FETCH_FORWARD` or `FETCH_BACKWARD`; absolute row number to move to for `FETCH_ABSOLUTE`; or relative row number to move to for `FETCH_RELATIVE`

## Return Value

`SPI_processed` is set as in `SPI_execute` if successful. `SPI_tuptable` is set to `NULL`, since no rows are returned by this function. 

## Notes

See the SQL [FETCH](sql-fetch.md "FETCH") command for details of the interpretation of the _`direction`_ and _`count`_ parameters. 

Direction values other than `FETCH_FORWARD` may fail if the cursor's plan was not created with the `CURSOR_OPT_SCROLL` option. 

* * *

[Prev](spi-spi-scroll-cursor-fetch.md "SPI_scroll_cursor_fetch") | [Up](spi-interface.md "45.1. Interface Functions")|  [Next](spi-spi-cursor-close.md "SPI_cursor_close")  
---|---|---  
SPI_scroll_cursor_fetch | [Home](index.md "PostgreSQL 17.5 Documentation")|  SPI_cursor_close
