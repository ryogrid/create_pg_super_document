SPI_getvalue  
---  
[Prev](spi-spi-fnumber.md "SPI_fnumber") | [Up](spi-interface-support.md "45.2. Interface Support Functions")| 45.2. Interface Support Functions| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](spi-spi-getbinval.md "SPI_getbinval")  
  
* * *

## SPI_getvalue

SPI_getvalue — return the string value of the specified column

## Synopsis
    
    
    char * SPI_getvalue(HeapTuple _row_ , TupleDesc _rowdesc_ , int _colnumber_)
    

## Description

`SPI_getvalue` returns the string representation of the value of the specified column. 

The result is returned in memory allocated using `palloc`. (You can use `pfree` to release the memory when you don't need it anymore.) 

## Arguments

`HeapTuple _`row`_`
    

input row to be examined 

`TupleDesc _`rowdesc`_`
    

input row description 

`int _`colnumber`_`
    

column number (count starts at 1) 

## Return Value

Column value, or `NULL` if the column is null, _`colnumber`_ is out of range (`SPI_result` is set to `SPI_ERROR_NOATTRIBUTE`), or no output function is available (`SPI_result` is set to `SPI_ERROR_NOOUTFUNC`). 

* * *

[Prev](spi-spi-fnumber.md "SPI_fnumber") | [Up](spi-interface-support.md "45.2. Interface Support Functions")|  [Next](spi-spi-getbinval.md "SPI_getbinval")  
---|---|---  
SPI_fnumber | [Home](index.md "PostgreSQL 17.5 Documentation")|  SPI_getbinval
