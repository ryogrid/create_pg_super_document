SPI_register_trigger_data  
---  
[Prev](spi-spi-unregister-relation.md "SPI_unregister_relation") | [Up](spi-interface.md "45.1. Interface Functions")| 45.1. Interface Functions| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](spi-interface-support.md "45.2. Interface Support Functions")  
  
* * *

## SPI_register_trigger_data

SPI_register_trigger_data — make ephemeral trigger data available in SPI queries

## Synopsis
    
    
    int SPI_register_trigger_data(TriggerData *_tdata_)
    

## Description

`SPI_register_trigger_data` makes any ephemeral relations captured by a trigger available to queries planned and executed through the current SPI connection. Currently, this means the transition tables captured by an `AFTER` trigger defined with a `REFERENCING OLD/NEW TABLE AS` ... clause. This function should be called by a PL trigger handler function after connecting. 

## Arguments

`TriggerData *_`tdata`_`
    

the `TriggerData` object passed to a trigger handler function as `fcinfo->context`

## Return Value

If the execution of the command was successful then the following (nonnegative) value will be returned: 

`SPI_OK_TD_REGISTER`
    

if the captured trigger data (if any) has been successfully registered 

On error, one of the following negative values is returned: 

`SPI_ERROR_ARGUMENT`
    

if _`tdata`_ is `NULL`

`SPI_ERROR_UNCONNECTED`
    

if called from an unconnected C function 

`SPI_ERROR_REL_DUPLICATE`
    

if the name of any trigger data transient relation is already registered for this connection 

* * *

[Prev](spi-spi-unregister-relation.md "SPI_unregister_relation") | [Up](spi-interface.md "45.1. Interface Functions")|  [Next](spi-interface-support.md "45.2. Interface Support Functions")  
---|---|---  
SPI_unregister_relation | [Home](index.md "PostgreSQL 17.5 Documentation")|  45.2. Interface Support Functions
