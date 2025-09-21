SPI_gettypeid  
---  
[Prev](spi-spi-gettype.md "SPI_gettype") | [Up](spi-interface-support.md "45.2. Interface Support Functions")| 45.2. Interface Support Functions| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](spi-spi-getrelname.md "SPI_getrelname")  
  
* * *

## SPI_gettypeid

SPI_gettypeid — return the data type OID of the specified column

## Synopsis
    
    
    Oid SPI_gettypeid(TupleDesc _rowdesc_ , int _colnumber_)
    

## Description

`SPI_gettypeid` returns the OID of the data type of the specified column. 

## Arguments

`TupleDesc _`rowdesc`_`
    

input row description 

`int _`colnumber`_`
    

column number (count starts at 1) 

## Return Value

The OID of the data type of the specified column or `InvalidOid` on error. On error, `SPI_result` is set to `SPI_ERROR_NOATTRIBUTE`. 

* * *

[Prev](spi-spi-gettype.md "SPI_gettype") | [Up](spi-interface-support.md "45.2. Interface Support Functions")|  [Next](spi-spi-getrelname.md "SPI_getrelname")  
---|---|---  
SPI_gettype | [Home](index.md "PostgreSQL 17.5 Documentation")|  SPI_getrelname
