SPI_fname  
---  
[Prev](spi-interface-support.md "45.2. Interface Support Functions") | [Up](spi-interface-support.md "45.2. Interface Support Functions")| 45.2. Interface Support Functions| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](spi-spi-fnumber.md "SPI_fnumber")  
  
* * *

## SPI_fname

SPI_fname — determine the column name for the specified column number

## Synopsis
    
    
    char * SPI_fname(TupleDesc _rowdesc_ , int _colnumber_)
    

## Description

`SPI_fname` returns a copy of the column name of the specified column. (You can use `pfree` to release the copy of the name when you don't need it anymore.) 

## Arguments

`TupleDesc _`rowdesc`_`
    

input row description 

`int _`colnumber`_`
    

column number (count starts at 1) 

## Return Value

The column name; `NULL` if _`colnumber`_ is out of range. `SPI_result` set to `SPI_ERROR_NOATTRIBUTE` on error. 

* * *

[Prev](spi-interface-support.md "45.2. Interface Support Functions") | [Up](spi-interface-support.md "45.2. Interface Support Functions")|  [Next](spi-spi-fnumber.md "SPI_fnumber")  
---|---|---  
45.2. Interface Support Functions | [Home](index.md "PostgreSQL 17.5 Documentation")|  SPI_fnumber
