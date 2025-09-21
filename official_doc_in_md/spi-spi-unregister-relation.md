SPI_unregister_relation  
---  
[Prev](spi-spi-register-relation.md "SPI_register_relation") | [Up](spi-interface.md "45.1. Interface Functions")| 45.1. Interface Functions| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](spi-spi-register-trigger-data.md "SPI_register_trigger_data")  
  
* * *

## SPI_unregister_relation

SPI_unregister_relation — remove an ephemeral named relation from the registry

## Synopsis
    
    
    int SPI_unregister_relation(const char * _name_)
    

## Description

`SPI_unregister_relation` removes an ephemeral named relation from the registry for the current connection. 

## Arguments

`const char * _`name`_`
    

the relation registry entry name 

## Return Value

If the execution of the command was successful then the following (nonnegative) value will be returned: 

`SPI_OK_REL_UNREGISTER`
    

if the tuplestore has been successfully removed from the registry 

On error, one of the following negative values is returned: 

`SPI_ERROR_ARGUMENT`
    

if _`name`_ is `NULL`

`SPI_ERROR_UNCONNECTED`
    

if called from an unconnected C function 

`SPI_ERROR_REL_NOT_FOUND`
    

if _`name`_ is not found in the registry for the current connection 

* * *

[Prev](spi-spi-register-relation.md "SPI_register_relation") | [Up](spi-interface.md "45.1. Interface Functions")|  [Next](spi-spi-register-trigger-data.md "SPI_register_trigger_data")  
---|---|---  
SPI_register_relation | [Home](index.md "PostgreSQL 17.5 Documentation")|  SPI_register_trigger_data
