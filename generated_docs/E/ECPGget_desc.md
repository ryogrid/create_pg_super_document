# ECPGget_desc

## Location
[src/interfaces/ecpg/ecpglib/descriptor.c:234-569](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/descriptor.c#L234-L569)

## Overview
ECPGget_desc retrieves various descriptor items from a prepared SQL statement result, allowing applications to extract metadata and result data from dynamic SQL queries.

## Definition

```c
enum ECPGdtype type;
```
## Detailed Description
ECPGget_desc is a variadic function that extracts descriptor information from PostgreSQL result sets in ECPG applications. It processes a variable number of descriptor type/variable pairs, allowing retrieval of column metadata (name, type, length, precision, scale), result data, and indicators. The function supports both static and dynamic memory allocation for result data and handles proper locale settings for numeric data conversion.

The function operates by:
1. Validating parameters and retrieving the result set associated with the descriptor
2. Processing variable argument pairs of descriptor types and target variables
3. Extracting requested information based on the descriptor type (metadata, data, indicators)
4. Handling memory allocation for dynamic arrays when needed
5. Converting and storing data with proper locale handling for numeric types

## Parameters / Member Variables
- : Source code line number for error reporting and debugging
- : Name of the descriptor to retrieve information from
- : 1-based column index in the result set to process
- : Variable arguments consisting of ECPGdtype/variable pairs terminated by ECPGd_EODT

## Dependencies
- Functions called/Symbols referenced:
  - ECPGget_sqlca
  - [ecpg_result_by_descriptor](../e/ecpg_result_by_descriptor.md)
  - [PQntuples](../P/PQntuples.md), PQnfields, PQfname, PQfsize, PQfmod, PQftype, PQgetlength, PQgetisnull
  - [get_char_item](../g/get_char_item.md), get_int_item
  - [ecpg_dynamic_type](../e/ecpg_dynamic_type.md), ecpg_dynamic_type_DDT
  - [ecpg_store_result](../e/ecpg_store_result.md)
  - [ecpg_auto_alloc](../e/ecpg_auto_alloc.md)
  - [ecpg_raise](../e/ecpg_raise.md), ecpg_log
- Called from (representative examples):
  - ECPG test programs (sql-describe.c, sql-dyntest.c, sql-dynalloc.c)
  - Dynamic SQL applications using DESCRIBE statements

## Notes and Other Information
- Supports multiple descriptor types: ECPGd_name, ECPGd_type, ECPGd_length, ECPGd_precision, ECPGd_scale, ECPGd_nullable, ECPGd_data, ECPGd_indicator, ECPGd_cardinality, ECPGd_ret_length, ECPGd_ret_octet
- Handles locale-specific numeric formatting by temporarily switching to C locale
- Provides automatic memory allocation for dynamic arrays when arrsize is 0
- Returns false on any error condition and sets appropriate SQLSTATE codes
- Thread-safe through proper locale handling mechanisms
- Critical for implementing SQL DESCRIBE functionality in ECPG applications