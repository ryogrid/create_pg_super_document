SPI_returntuple  
---  
[Prev](spi-spi-copytuple.md "SPI_copytuple") | [Up](spi-memory.md "45.3. Memory Management")| 45.3. Memory Management| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](spi-spi-modifytuple.md "SPI_modifytuple")  
  
* * *

## SPI_returntuple

SPI_returntuple — prepare to return a tuple as a Datum

## Synopsis
    
    
    HeapTupleHeader SPI_returntuple(HeapTuple _row_ , TupleDesc _rowdesc_)
    

## Description

`SPI_returntuple` makes a copy of a row in the upper executor context, returning it in the form of a row type `Datum`. The returned pointer need only be converted to `Datum` via `PointerGetDatum` before returning. 

This function can only be used while connected to SPI. Otherwise, it returns NULL and sets `SPI_result` to `SPI_ERROR_UNCONNECTED`. 

Note that this should be used for functions that are declared to return composite types. It is not used for triggers; use `SPI_copytuple` for returning a modified row in a trigger. 

## Arguments

`HeapTuple _`row`_`
    

row to be copied 

`TupleDesc _`rowdesc`_`
    

descriptor for row (pass the same descriptor each time for most effective caching) 

## Return Value

`HeapTupleHeader` pointing to copied row, or `NULL` on error (see `SPI_result` for an error indication) 

* * *

[Prev](spi-spi-copytuple.md "SPI_copytuple") | [Up](spi-memory.md "45.3. Memory Management")|  [Next](spi-spi-modifytuple.md "SPI_modifytuple")  
---|---|---  
SPI_copytuple | [Home](index.md "PostgreSQL 17.5 Documentation")|  SPI_modifytuple
