SPI_cursor_find  
---  
[Prev](spi-spi-cursor-parse-open.md "SPI_cursor_parse_open") | [Up](spi-interface.md "45.1. Interface Functions")| 45.1. Interface Functions| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](spi-spi-cursor-fetch.md "SPI_cursor_fetch")  
  
* * *

## SPI_cursor_find

SPI_cursor_find — find an existing cursor by name

## Synopsis
    
    
    Portal SPI_cursor_find(const char * _name_)
    

## Description

`SPI_cursor_find` finds an existing portal by name. This is primarily useful to resolve a cursor name returned as text by some other function. 

## Arguments

`const char * _`name`_`
    

name of the portal 

## Return Value

pointer to the portal with the specified name, or `NULL` if none was found 

## Notes

Beware that this function can return a `Portal` object that does not have cursor-like properties; for example it might not return tuples. If you simply pass the `Portal` pointer to other SPI functions, they can defend themselves against such cases, but caution is appropriate when directly inspecting the `Portal`. 

* * *

[Prev](spi-spi-cursor-parse-open.md "SPI_cursor_parse_open") | [Up](spi-interface.md "45.1. Interface Functions")|  [Next](spi-spi-cursor-fetch.md "SPI_cursor_fetch")  
---|---|---  
SPI_cursor_parse_open | [Home](index.md "PostgreSQL 17.5 Documentation")|  SPI_cursor_fetch
