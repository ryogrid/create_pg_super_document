SPI_cursor_open_with_args  
---  
[Prev](spi-spi-cursor-open.md "SPI_cursor_open") | [Up](spi-interface.md "45.1. Interface Functions")| 45.1. Interface Functions| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](spi-spi-cursor-open-with-paramlist.md "SPI_cursor_open_with_paramlist")  
  
* * *

## SPI_cursor_open_with_args

SPI_cursor_open_with_args — set up a cursor using a query and parameters

## Synopsis
    
    
    Portal SPI_cursor_open_with_args(const char *_name_ ,
                                     const char *_command_ ,
                                     int _nargs_ , Oid *_argtypes_ ,
                                     Datum *_values_ , const char *_nulls_ ,
                                     bool _read_only_ , int _cursorOptions_)
    

## Description

`SPI_cursor_open_with_args` sets up a cursor (internally, a portal) that will execute the specified query. Most of the parameters have the same meanings as the corresponding parameters to `SPI_prepare_cursor` and `SPI_cursor_open`. 

For one-time query execution, this function should be preferred over `SPI_prepare_cursor` followed by `SPI_cursor_open`. If the same command is to be executed with many different parameters, either method might be faster, depending on the cost of re-planning versus the benefit of custom plans. 

The passed-in parameter data will be copied into the cursor's portal, so it can be freed while the cursor still exists. 

This function is now deprecated in favor of `SPI_cursor_parse_open`, which provides equivalent functionality using a more modern API for handling query parameters. 

## Arguments

`const char * _`name`_`
    

name for portal, or `NULL` to let the system select a name 

`const char * _`command`_`
    

command string 

`int _`nargs`_`
    

number of input parameters (`$1`, `$2`, etc.) 

`Oid * _`argtypes`_`
    

an array of length _`nargs`_ , containing the OIDs of the data types of the parameters 

`Datum * _`values`_`
    

an array of length _`nargs`_ , containing the actual parameter values 

`const char * _`nulls`_`
    

an array of length _`nargs`_ , describing which parameters are null 

If _`nulls`_ is `NULL` then `SPI_cursor_open_with_args` assumes that no parameters are null. Otherwise, each entry of the _`nulls`_ array should be `' '` if the corresponding parameter value is non-null, or `'n'` if the corresponding parameter value is null. (In the latter case, the actual value in the corresponding _`values`_ entry doesn't matter.) Note that _`nulls`_ is not a text string, just an array: it does not need a `'\0'` terminator. 

`bool _`read_only`_`
    

`true` for read-only execution

`int _`cursorOptions`_`
    

integer bit mask of cursor options; zero produces default behavior 

## Return Value

Pointer to portal containing the cursor. Note there is no error return convention; any error will be reported via `elog`. 

* * *

[Prev](spi-spi-cursor-open.md "SPI_cursor_open") | [Up](spi-interface.md "45.1. Interface Functions")|  [Next](spi-spi-cursor-open-with-paramlist.md "SPI_cursor_open_with_paramlist")  
---|---|---  
SPI_cursor_open | [Home](index.md "PostgreSQL 17.5 Documentation")|  SPI_cursor_open_with_paramlist
