SPI_execute_with_args  
---  
[Prev](spi-spi-execute-extended.md "SPI_execute_extended") | [Up](spi-interface.md "45.1. Interface Functions")| 45.1. Interface Functions| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](spi-spi-prepare.md "SPI_prepare")  
  
* * *

## SPI_execute_with_args

SPI_execute_with_args — execute a command with out-of-line parameters

## Synopsis
    
    
    int SPI_execute_with_args(const char *_command_ ,
                              int _nargs_ , Oid *_argtypes_ ,
                              Datum *_values_ , const char *_nulls_ ,
                              bool _read_only_ , long _count_)
    

## Description

`SPI_execute_with_args` executes a command that might include references to externally supplied parameters. The command text refers to a parameter as `$_`n`_`, and the call specifies data types and values for each such symbol. _`read_only`_ and _`count`_ have the same interpretation as in `SPI_execute`. 

The main advantage of this routine compared to `SPI_execute` is that data values can be inserted into the command without tedious quoting/escaping, and thus with much less risk of SQL-injection attacks. 

Similar results can be achieved with `SPI_prepare` followed by `SPI_execute_plan`; however, when using this function the query plan is always customized to the specific parameter values provided. For one-time query execution, this function should be preferred. If the same command is to be executed with many different parameters, either method might be faster, depending on the cost of re-planning versus the benefit of custom plans. 

## Arguments

`const char * _`command`_`
    

command string 

`int _`nargs`_`
    

number of input parameters (`$1`, `$2`, etc.) 

`Oid * _`argtypes`_`
    

an array of length _`nargs`_ , containing the OIDs of the data types of the parameters 

`Datum * _`values`_`
    

an array of length _`nargs`_ , containing the actual parameter values 

`const char * _`nulls`_`
    

an array of length _`nargs`_ , describing which parameters are null 

If _`nulls`_ is `NULL` then `SPI_execute_with_args` assumes that no parameters are null. Otherwise, each entry of the _`nulls`_ array should be `' '` if the corresponding parameter value is non-null, or `'n'` if the corresponding parameter value is null. (In the latter case, the actual value in the corresponding _`values`_ entry doesn't matter.) Note that _`nulls`_ is not a text string, just an array: it does not need a `'\0'` terminator. 

`bool _`read_only`_`
    

`true` for read-only execution

`long _`count`_`
    

maximum number of rows to return, or `0` for no limit 

## Return Value

The return value is the same as for `SPI_execute`. 

`SPI_processed` and `SPI_tuptable` are set as in `SPI_execute` if successful. 

* * *

[Prev](spi-spi-execute-extended.md "SPI_execute_extended") | [Up](spi-interface.md "45.1. Interface Functions")|  [Next](spi-spi-prepare.md "SPI_prepare")  
---|---|---  
SPI_execute_extended | [Home](index.md "PostgreSQL 17.5 Documentation")|  SPI_prepare
