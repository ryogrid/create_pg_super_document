# typeByVal

## Location
[src/backend/parser/parse_type.c:609-618](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_type.c#L609-L618)

## Overview
Returns the 'byval' attribute of a PostgreSQL data type, indicating whether values of this type are passed by value rather than by reference.

## Definition


## Detailed Description
The  function extracts the  attribute from a PostgreSQL type structure. This boolean attribute determines how values of the type are passed in function calls and stored in memory. When  is true, values are passed by value (copied), which is typically used for small, fixed-size types like integers. When false, values are passed by reference (pointer), which is used for variable-length or large types like text strings.

This function provides a clean interface to access the  field from the  system catalog without requiring direct manipulation of the type structure.

## Parameters / Member Variables
- : A Type structure (HeapTuple) representing a row from the pg_type system catalog

## Dependencies
- Functions called/Symbols referenced:
  - Type (typedef for HeapTuple)
  - Form_pg_type (structure representing pg_type catalog row)
  - GETSTRUCT (macro to extract structure from HeapTuple)
- Called from (representative examples):
  - [coerce_type](../c/coerce_type.md) (in parse_coerce.c:290)

## Notes and Other Information
- This is a utility function that abstracts access to the pg_type catalog structure
- The 'byval' attribute is fundamental to PostgreSQL's type system and affects performance and memory management
- Types that are passed by value are typically limited to a maximum size (usually 8 bytes on 64-bit systems)
- This function is part of the parser subsystem's type handling utilities