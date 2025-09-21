SPI_getrelname  
---  
[Prev](spi-spi-gettypeid.md "SPI_gettypeid") | [Up](spi-interface-support.md "45.2. Interface Support Functions")| 45.2. Interface Support Functions| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](spi-spi-getnspname.md "SPI_getnspname")  
  
* * *

## SPI_getrelname

SPI_getrelname — return the name of the specified relation

## Synopsis
    
    
    char * SPI_getrelname(Relation _rel_)
    

## Description

`SPI_getrelname` returns a copy of the name of the specified relation. (You can use `pfree` to release the copy of the name when you don't need it anymore.) 

## Arguments

`Relation _`rel`_`
    

input relation 

## Return Value

The name of the specified relation. 

* * *

[Prev](spi-spi-gettypeid.md "SPI_gettypeid") | [Up](spi-interface-support.md "45.2. Interface Support Functions")|  [Next](spi-spi-getnspname.md "SPI_getnspname")  
---|---|---  
SPI_gettypeid | [Home](index.md "PostgreSQL 17.5 Documentation")|  SPI_getnspname
