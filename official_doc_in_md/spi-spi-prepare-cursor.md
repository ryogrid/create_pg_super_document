SPI_prepare_cursor  
---  
[Prev](spi-spi-prepare.md "SPI_prepare") | [Up](spi-interface.md "45.1. Interface Functions")| 45.1. Interface Functions| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](spi-spi-prepare-extended.md "SPI_prepare_extended")  
  
* * *

## SPI_prepare_cursor

SPI_prepare_cursor — prepare a statement, without executing it yet

## Synopsis
    
    
    SPIPlanPtr SPI_prepare_cursor(const char * _command_ , int _nargs_ ,
                                  Oid * _argtypes_ , int _cursorOptions_)
    

## Description

`SPI_prepare_cursor` is identical to `SPI_prepare`, except that it also allows specification of the planner's “cursor options” parameter. This is a bit mask having the values shown in `nodes/parsenodes.h` for the `options` field of `DeclareCursorStmt`. `SPI_prepare` always takes the cursor options as zero. 

This function is now deprecated in favor of `SPI_prepare_extended`. 

## Arguments

`const char * _`command`_`
    

command string 

`int _`nargs`_`
    

number of input parameters (`$1`, `$2`, etc.) 

`Oid * _`argtypes`_`
    

pointer to an array containing the OIDs of the data types of the parameters 

`int _`cursorOptions`_`
    

integer bit mask of cursor options; zero produces default behavior 

## Return Value

`SPI_prepare_cursor` has the same return conventions as `SPI_prepare`. 

## Notes

Useful bits to set in _`cursorOptions`_ include `CURSOR_OPT_SCROLL`, `CURSOR_OPT_NO_SCROLL`, `CURSOR_OPT_FAST_PLAN`, `CURSOR_OPT_GENERIC_PLAN`, and `CURSOR_OPT_CUSTOM_PLAN`. Note in particular that `CURSOR_OPT_HOLD` is ignored. 

* * *

[Prev](spi-spi-prepare.md "SPI_prepare") | [Up](spi-interface.md "45.1. Interface Functions")|  [Next](spi-spi-prepare-extended.md "SPI_prepare_extended")  
---|---|---  
SPI_prepare | [Home](index.md "PostgreSQL 17.5 Documentation")|  SPI_prepare_extended
