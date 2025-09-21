SPI_getnspname  
---  
[Prev](spi-spi-getrelname.md "SPI_getrelname") | [Up](spi-interface-support.md "45.2. Interface Support Functions")| 45.2. Interface Support Functions| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](spi-spi-result-code-string.md "SPI_result_code_string")  
  
* * *

## SPI_getnspname

SPI_getnspname — return the namespace of the specified relation

## Synopsis
    
    
    char * SPI_getnspname(Relation _rel_)
    

## Description

`SPI_getnspname` returns a copy of the name of the namespace that the specified `Relation` belongs to. This is equivalent to the relation's schema. You should `pfree` the return value of this function when you are finished with it. 

## Arguments

`Relation _`rel`_`
    

input relation 

## Return Value

The name of the specified relation's namespace. 

* * *

[Prev](spi-spi-getrelname.md "SPI_getrelname") | [Up](spi-interface-support.md "45.2. Interface Support Functions")|  [Next](spi-spi-result-code-string.md "SPI_result_code_string")  
---|---|---  
SPI_getrelname | [Home](index.md "PostgreSQL 17.5 Documentation")|  SPI_result_code_string
