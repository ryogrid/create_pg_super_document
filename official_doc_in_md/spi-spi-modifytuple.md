SPI_modifytuple  
---  
[Prev](spi-spi-returntuple.md "SPI_returntuple") | [Up](spi-memory.md "45.3. Memory Management")| 45.3. Memory Management| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](spi-spi-freetuple.md "SPI_freetuple")  
  
* * *

## SPI_modifytuple

SPI_modifytuple — create a row by replacing selected fields of a given row

## Synopsis
    
    
    HeapTuple SPI_modifytuple(Relation _rel_ , HeapTuple _row_ , int _ncols_ ,
                              int * _colnum_ , Datum * _values_ , const char * _nulls_)
    

## Description

`SPI_modifytuple` creates a new row by substituting new values for selected columns, copying the original row's columns at other positions. The input row is not modified. The new row is returned in the upper executor context. 

This function can only be used while connected to SPI. Otherwise, it returns NULL and sets `SPI_result` to `SPI_ERROR_UNCONNECTED`. 

## Arguments

`Relation _`rel`_`
    

Used only as the source of the row descriptor for the row. (Passing a relation rather than a row descriptor is a misfeature.) 

`HeapTuple _`row`_`
    

row to be modified 

`int _`ncols`_`
    

number of columns to be changed 

`int * _`colnum`_`
    

an array of length _`ncols`_ , containing the numbers of the columns that are to be changed (column numbers start at 1) 

`Datum * _`values`_`
    

an array of length _`ncols`_ , containing the new values for the specified columns 

`const char * _`nulls`_`
    

an array of length _`ncols`_ , describing which new values are null 

If _`nulls`_ is `NULL` then `SPI_modifytuple` assumes that no new values are null. Otherwise, each entry of the _`nulls`_ array should be `' '` if the corresponding new value is non-null, or `'n'` if the corresponding new value is null. (In the latter case, the actual value in the corresponding _`values`_ entry doesn't matter.) Note that _`nulls`_ is not a text string, just an array: it does not need a `'\0'` terminator. 

## Return Value

new row with modifications, allocated in the upper executor context, or `NULL` on error (see `SPI_result` for an error indication) 

On error, `SPI_result` is set as follows: 

`SPI_ERROR_ARGUMENT`
    

if _`rel`_ is `NULL`, or if _`row`_ is `NULL`, or if _`ncols`_ is less than or equal to 0, or if _`colnum`_ is `NULL`, or if _`values`_ is `NULL`. 

`SPI_ERROR_NOATTRIBUTE`
    

if _`colnum`_ contains an invalid column number (less than or equal to 0 or greater than the number of columns in _`row`_) 

`SPI_ERROR_UNCONNECTED`
    

if SPI is not active 

* * *

[Prev](spi-spi-returntuple.md "SPI_returntuple") | [Up](spi-memory.md "45.3. Memory Management")|  [Next](spi-spi-freetuple.md "SPI_freetuple")  
---|---|---  
SPI_returntuple | [Home](index.md "PostgreSQL 17.5 Documentation")|  SPI_freetuple
