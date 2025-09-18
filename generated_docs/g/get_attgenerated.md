# get_attgenerated

## Location
src/backend/utils/cache/lsyscache.c: 888 - 912

## Overview
Retrieves the generation status of an attribute from the PostgreSQL system catalog, indicating whether a column is a generated column and its type.

## Definition
```c
char get_attgenerated(Oid relid, AttrNumber attnum)
```

## Detailed Description
This function performs a system cache lookup to retrieve the attgenerated field from the pg_attribute catalog table. The attgenerated field indicates whether a column is a generated column and what type of generation it uses. The function returns a character value where '\\0' represents a regular (non-generated) column, and other values indicate different types of generated columns. Unlike some other attribute functions, this function always throws an error if the attribute is not found.

## Parameters / Member Variables
- `relid`: Object identifier of the relation (table/view/etc.) containing the attribute
- `attnum`: Attribute number (column number) within the relation

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache2 - performs the system cache lookup using ATTNUM cache
  - ObjectIdGetDatum - converts relation OID to Datum format
  - Int16GetDatum - converts attribute number to Datum format
  - HeapTupleIsValid - checks if the cache lookup returned a valid tuple
  - GETSTRUCT - extracts the Form_pg_attribute structure from the heap tuple
  - ReleaseSysCache - releases the system cache entry
  - elog - logs error messages when attribute is not found

- Called from (representative examples):
  - check_nested_generated_walker (catalog/heap.c:2764)

## Notes and Other Information
- Always throws an error if the attribute is not found, unlike some other attribute functions
- Returns '\\0' for regular (non-generated) columns, making it usable as a Boolean test
- Can return different character codes for different types of generated columns
- Part of PostgreSQL's generated column functionality introduced in version 12
- The returned value can be used directly in Boolean contexts since '\\0' evaluates to false
- Essential for validating and processing generated column constraints and dependencies
- Used primarily in DDL operations and constraint validation logic