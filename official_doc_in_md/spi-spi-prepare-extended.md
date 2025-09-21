SPI_prepare_extended  
---  
[Prev](spi-spi-prepare-cursor.md "SPI_prepare_cursor") | [Up](spi-interface.md "45.1. Interface Functions")| 45.1. Interface Functions| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](spi-spi-prepare-params.md "SPI_prepare_params")  
  
* * *

## SPI_prepare_extended

SPI_prepare_extended — prepare a statement, without executing it yet

## Synopsis
    
    
    SPIPlanPtr SPI_prepare_extended(const char * _command_ ,
                                    const SPIPrepareOptions * _options_)
    

## Description

`SPI_prepare_extended` creates and returns a prepared statement for the specified command, but doesn't execute the command. This function is equivalent to `SPI_prepare`, with the addition that the caller can specify options to control the parsing of external parameter references, as well as other facets of query parsing and planning. 

## Arguments

`const char * _`command`_`
    

command string 

`const SPIPrepareOptions * _`options`_`
    

struct containing optional arguments 

Callers should always zero out the entire _`options`_ struct, then fill whichever fields they want to set. This ensures forward compatibility of code, since any fields that are added to the struct in future will be defined to behave backwards-compatibly if they are zero. The currently available _`options`_ fields are: 

`ParserSetupHook _`parserSetup`_`
    

Parser hook setup function 

`void * _`parserSetupArg`_`
    

pass-through argument for _`parserSetup`_

`RawParseMode _`parseMode`_`
    

mode for raw parsing; `RAW_PARSE_DEFAULT` (zero) produces default behavior 

`int _`cursorOptions`_`
    

integer bit mask of cursor options; zero produces default behavior 

## Return Value

`SPI_prepare_extended` has the same return conventions as `SPI_prepare`. 

* * *

[Prev](spi-spi-prepare-cursor.md "SPI_prepare_cursor") | [Up](spi-interface.md "45.1. Interface Functions")|  [Next](spi-spi-prepare-params.md "SPI_prepare_params")  
---|---|---  
SPI_prepare_cursor | [Home](index.md "PostgreSQL 17.5 Documentation")|  SPI_prepare_params
