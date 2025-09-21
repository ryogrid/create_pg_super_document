SPI_getbinval  
---  
[Prev](spi-spi-getvalue.md "SPI_getvalue") | [Up](spi-interface-support.md "45.2. Interface Support Functions")| 45.2. Interface Support Functions| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](spi-spi-gettype.md "SPI_gettype")  
  
* * *

## SPI_getbinval

SPI_getbinval — return the binary value of the specified column

## Synopsis
    
    
    Datum SPI_getbinval(HeapTuple _row_ , TupleDesc _rowdesc_ , int _colnumber_ ,
                        bool * _isnull_)
    

## Description

`SPI_getbinval` returns the value of the specified column in the internal form (as type `Datum`). 

This function does not allocate new space for the datum. In the case of a pass-by-reference data type, the return value will be a pointer into the passed row. 

## Arguments

`HeapTuple _`row`_`
    

input row to be examined 

`TupleDesc _`rowdesc`_`
    

input row description 

`int _`colnumber`_`
    

column number (count starts at 1) 

`bool * _`isnull`_`
    

flag for a null value in the column 

## Return Value

The binary value of the column is returned. The variable pointed to by _`isnull`_ is set to true if the column is null, else to false. 

`SPI_result` is set to `SPI_ERROR_NOATTRIBUTE` on error. 

* * *

[Prev](spi-spi-getvalue.md "SPI_getvalue") | [Up](spi-interface-support.md "45.2. Interface Support Functions")|  [Next](spi-spi-gettype.md "SPI_gettype")  
---|---|---  
SPI_getvalue | [Home](index.md "PostgreSQL 17.5 Documentation")|  SPI_gettype
