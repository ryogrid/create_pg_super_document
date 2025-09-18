# ExceptionMap

## Location
[src/pl/plpython/plpy_plpymodule.c:46-51](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_plpymodule.c#L46-L51)

## Overview
A structure that maps PostgreSQL SQL state error codes to Python exception classes in the PL/Python language handler.

## Definition


## Detailed Description
ExceptionMap is a structure used by PostgreSQL's PL/Python extension to maintain a mapping between PostgreSQL's internal SQL state error codes and their corresponding Python exception classes. This structure is part of the exception handling system that allows Python code running within PostgreSQL to catch and handle database errors using Python's exception mechanisms.

The structure is used to populate an array of exception mappings that are automatically generated from PostgreSQL's backend/utils/errcodes.txt file. These mappings enable the PL/Python extension to convert PostgreSQL error conditions into appropriate Python exceptions that can be caught and handled by Python stored procedures and functions.

## Parameters / Member Variables
- : A string containing the name of the PostgreSQL error condition
- : A string containing the corresponding Python exception class name
- : An integer representing the PostgreSQL SQL state error code

## Dependencies
- Functions called/Symbols referenced:
  - Used in static array  in plpy_plpymodule.c:53
- Called from (representative examples):
  - [PLy_generate_spi_exceptions](../P/PLy_generate_spi_exceptions.md) (iterates through exception_map array)
  - Hash table lookups in PLy_spi_exceptions

## Notes and Other Information
- The ExceptionMap structure is defined in src/pl/plpython/plpy_plpymodule.c:46-51
- It is used as part of an array that includes auto-generated content from spiexceptions.h 
- The array is terminated with a NULL entry: {NULL, NULL, 0}
- This mapping system enables seamless integration between PostgreSQL's error handling and Python's exception model
- The structure supports the PL/Python extension's ability to provide meaningful Python exceptions for database errors