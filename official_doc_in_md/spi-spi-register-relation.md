SPI_register_relation  
---  
[Prev](spi-spi-saveplan.md "SPI_saveplan") | [Up](spi-interface.md "45.1. Interface Functions")| 45.1. Interface Functions| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](spi-spi-unregister-relation.md "SPI_unregister_relation")  
  
* * *

## SPI_register_relation

SPI_register_relation — make an ephemeral named relation available by name in SPI queries

## Synopsis
    
    
    int SPI_register_relation(EphemeralNamedRelation _enr_)
    

## Description

`SPI_register_relation` makes an ephemeral named relation, with associated information, available to queries planned and executed through the current SPI connection. 

## Arguments

`EphemeralNamedRelation _`enr`_`
    

the ephemeral named relation registry entry 

## Return Value

If the execution of the command was successful then the following (nonnegative) value will be returned: 

`SPI_OK_REL_REGISTER`
    

if the relation has been successfully registered by name 

On error, one of the following negative values is returned: 

`SPI_ERROR_ARGUMENT`
    

if _`enr`_ is `NULL` or its `name` field is `NULL`

`SPI_ERROR_UNCONNECTED`
    

if called from an unconnected C function 

`SPI_ERROR_REL_DUPLICATE`
    

if the name specified in the `name` field of _`enr`_ is already registered for this connection 

* * *

[Prev](spi-spi-saveplan.md "SPI_saveplan") | [Up](spi-interface.md "45.1. Interface Functions")|  [Next](spi-spi-unregister-relation.md "SPI_unregister_relation")  
---|---|---  
SPI_saveplan | [Home](index.md "PostgreSQL 17.5 Documentation")|  SPI_unregister_relation
