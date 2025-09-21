SPI_fnumber  
---  
[Prev](spi-spi-fname.md "SPI_fname") | [Up](spi-interface-support.md "45.2. Interface Support Functions")| 45.2. Interface Support Functions| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](spi-spi-getvalue.md "SPI_getvalue")  
  
* * *

## SPI_fnumber

SPI_fnumber — determine the column number for the specified column name

## Synopsis
    
    
    int SPI_fnumber(TupleDesc _rowdesc_ , const char * _colname_)
    

## Description

`SPI_fnumber` returns the column number for the column with the specified name. 

If _`colname`_ refers to a system column (e.g., `ctid`) then the appropriate negative column number will be returned. The caller should be careful to test the return value for exact equality to `SPI_ERROR_NOATTRIBUTE` to detect an error; testing the result for less than or equal to 0 is not correct unless system columns should be rejected. 

## Arguments

`TupleDesc _`rowdesc`_`
    

input row description 

`const char * _`colname`_`
    

column name 

## Return Value

Column number (count starts at 1 for user-defined columns), or `SPI_ERROR_NOATTRIBUTE` if the named column was not found. 

* * *

[Prev](spi-spi-fname.md "SPI_fname") | [Up](spi-interface-support.md "45.2. Interface Support Functions")|  [Next](spi-spi-getvalue.md "SPI_getvalue")  
---|---|---  
SPI_fname | [Home](index.md "PostgreSQL 17.5 Documentation")|  SPI_getvalue
