SPI_prepare_params  
---  
[Prev](spi-spi-prepare-extended.md "SPI_prepare_extended") | [Up](spi-interface.md "45.1. Interface Functions")| 45.1. Interface Functions| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](spi-spi-getargcount.md "SPI_getargcount")  
  
* * *

## SPI_prepare_params

SPI_prepare_params — prepare a statement, without executing it yet

## Synopsis
    
    
    SPIPlanPtr SPI_prepare_params(const char * _command_ ,
                                  ParserSetupHook _parserSetup_ ,
                                  void * _parserSetupArg_ ,
                                  int _cursorOptions_)
    

## Description

`SPI_prepare_params` creates and returns a prepared statement for the specified command, but doesn't execute the command. This function is equivalent to `SPI_prepare_cursor`, with the addition that the caller can specify parser hook functions to control the parsing of external parameter references. 

This function is now deprecated in favor of `SPI_prepare_extended`. 

## Arguments

`const char * _`command`_`
    

command string 

`ParserSetupHook _`parserSetup`_`
    

Parser hook setup function 

`void * _`parserSetupArg`_`
    

pass-through argument for _`parserSetup`_

`int _`cursorOptions`_`
    

integer bit mask of cursor options; zero produces default behavior 

## Return Value

`SPI_prepare_params` has the same return conventions as `SPI_prepare`. 

* * *

[Prev](spi-spi-prepare-extended.md "SPI_prepare_extended") | [Up](spi-interface.md "45.1. Interface Functions")|  [Next](spi-spi-getargcount.md "SPI_getargcount")  
---|---|---  
SPI_prepare_extended | [Home](index.md "PostgreSQL 17.5 Documentation")|  SPI_getargcount
