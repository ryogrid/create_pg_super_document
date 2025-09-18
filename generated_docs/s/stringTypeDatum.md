# stringTypeDatum

## Location
src/backend/parser/parse_type.c: 654 - 667

## Overview
Converts a string representation of a value to its internal PostgreSQL Datum representation for a given data type.

## Definition


## Detailed Description
The  function performs type input conversion by taking a string representation of a value and converting it to PostgreSQL's internal Datum format according to the specified type. This function is a key component of PostgreSQL's type system, enabling the conversion from human-readable text representations to the internal binary format used for storage and computation.

The function extracts the type's input function () from the type structure and calls it through  along with appropriate parameters. The input function is responsible for parsing the string according to the type's specific format rules and producing the corresponding internal representation.

The function can handle NULL input strings, though the behavior depends on whether the specific type's input function accepts NULL values.

## Parameters / Member Variables
- : A Type structure (HeapTuple) representing a row from the pg_type system catalog
- : The string representation of the value to convert (can be NULL)
- : Type modifier value that provides additional type-specific information (e.g., precision for numeric types, length limits for varchar)

## Dependencies
- Functions called/Symbols referenced:
  - Type (typedef for HeapTuple)
  - Form_pg_type (structure representing pg_type catalog row)
  - GETSTRUCT (macro to extract structure from HeapTuple)
  - getTypeIOParam (function to get the I/O parameter for the type)
  - OidInputFunctionCall (function to call the type's input function)
  - Datum (PostgreSQL's internal value representation type)
  - Oid (object identifier type)
- Called from (representative examples):
  - coerce_type (in parse_coerce.c:311, 315, 345)

## Notes and Other Information
- This function is essential for literal value parsing and type coercion in PostgreSQL
- The atttypmod parameter allows for type-specific constraints (e.g., VARCHAR(50) has typmod = 54)
- Each PostgreSQL data type has its own input function that knows how to parse strings in that type's format
- The function may raise errors if the string cannot be parsed according to the type's rules
- This function is part of the parser subsystem's type handling utilities
- The returned Datum may need to be freed depending on the type (for pass-by-reference types)