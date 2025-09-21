SPI_gettype  
---  
[Prev](spi-spi-getbinval.md "SPI_getbinval") | [Up](spi-interface-support.md "45.2. Interface Support Functions")| 45.2. Interface Support Functions| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](spi-spi-gettypeid.md "SPI_gettypeid")  
  
* * *

## SPI_gettype

SPI_gettype — return the data type name of the specified column

## Synopsis
    
    
    char * SPI_gettype(TupleDesc _rowdesc_ , int _colnumber_)
    

## Description

`SPI_gettype` returns a copy of the data type name of the specified column. (You can use `pfree` to release the copy of the name when you don't need it anymore.) 

## Arguments

`TupleDesc _`rowdesc`_`
    

input row description 

`int _`colnumber`_`
    

column number (count starts at 1) 

## Return Value

The data type name of the specified column, or `NULL` on error. `SPI_result` is set to `SPI_ERROR_NOATTRIBUTE` on error. 

* * *

[Prev](spi-spi-getbinval.md "SPI_getbinval") | [Up](spi-interface-support.md "45.2. Interface Support Functions")|  [Next](spi-spi-gettypeid.md "SPI_gettypeid")  
---|---|---  
SPI_getbinval | [Home](index.md "PostgreSQL 17.5 Documentation")|  SPI_gettypeid
