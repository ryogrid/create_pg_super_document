# risnull

## Location
[src/interfaces/ecpg/compatlib/informix.c:1049-1052](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/compatlib/informix.c#L1049-L1052)

## Overview
Checks whether a value is NULL for a given data type in the Informix compatibility layer of PostgreSQL's ECPG (Embedded SQL in C) interface.

## Definition


## Detailed Description
The  function is part of PostgreSQL's ECPG Informix compatibility library. It provides a wrapper around the ECPG library's  function to check if a value is NULL for a specified data type. This function maintains compatibility with Informix's SQL APIs by providing the same interface that Informix developers would expect.

The function returns an integer indicating whether the value at the specified memory location is NULL (non-zero) or not NULL (zero). The actual NULL-checking logic is delegated to the underlying ECPG library function .

## Parameters / Member Variables
- : Integer representing the data type identifier for the value to be checked
- : Pointer to the memory location where the value should be checked for NULL

## Dependencies
- Functions called/Symbols referenced:
  - [ECPGis_noind_null](../E/ECPGis_noind_null.md)
- Called from (representative examples):
  - [deccall3](../d/deccall3.md)
  - [deccvasc](../d/deccvasc.md)
  - [deccvdbl](../d/deccvdbl.md)
  - [deccvint](../d/deccvint.md)
  - [deccvlong](../d/deccvlong.md)
  - [dectoasc](../d/dectoasc.md)
  - [test_null](../t/test_null.md) (in test cases)
  - Various merge join functions in the PostgreSQL executor
  - Various test functions in the ECPG test suite

## Notes and Other Information
- This function is declared in 
- It's part of the Informix compatibility layer, allowing existing Informix applications to be more easily ported to PostgreSQL
- Returns non-zero if the value is NULL, zero if the value is not NULL
- Used extensively in decimal/numeric conversion functions and NULL value testing
- Works in conjunction with  which sets a value to NULL
- The function is also used by PostgreSQL's internal merge join executor for NULL value comparisons
- The  parameter is declared as  indicating the function does not modify the data being checked