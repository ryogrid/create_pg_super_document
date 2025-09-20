# readDatum

## Location
[src/backend/nodes/readfuncs.c:589-649](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/readfuncs.c#L589-L649)

## Overview
A function that reconstructs a Datum value from its string representation during PostgreSQL node deserialization, handling both by-value and by-reference data types.

## Definition

```c
Datum
readDatum(bool typbyval)
```
## Detailed Description
The  function is a core utility in PostgreSQL's node deserialization system that converts string representations of constant values back into Datum format. A Datum is PostgreSQL's universal data container that can hold values of any data type.

The function handles the serialized format which embeds length information but requires the caller to specify whether the type is passed by value (). The serialized format follows the pattern:  where each byte is represented as a decimal number.

The function processes two main categories:
- **By-value types** (): Small values that fit within a Datum (typically <= 8 bytes on 64-bit systems). These are stored directly in the Datum value itself.
- **By-reference types** (): Larger values stored as pointers. The function allocates memory and stores a pointer to the data in the Datum.

Special handling includes:
- Zero-length by-reference data results in a NULL Datum
- Validation of bracket delimiters '[' and ']' around the byte sequence
- Error checking for oversized by-value data
- Proper memory allocation for by-reference data using 

## Parameters / Member Variables
- : Boolean indicating whether the data type is passed by value (true) or by reference (false)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_strtok](../p/pg_strtok.md) (tokenizer function for parsing serialized data)
  - atoui (ASCII to unsigned integer conversion)
  - atoi (ASCII to integer conversion)
  - [palloc](../p/palloc.md) (PostgreSQL memory allocator)
  - [PointerGetDatum](../P/PointerGetDatum.md) (macro to convert pointer to Datum)
  - elog (error logging function)
- Called from (representative examples):
  - [_readConst](_readConst.md) (in readfuncs.c:275)

## Notes and Other Information
- This is a non-static function, making it available to other compilation units
- Critical for deserializing constant values in cached query plans and parallel query execution
- The serialized format stores individual bytes as decimal strings, not as a continuous hex or binary string
- Memory allocated for by-reference types is managed by PostgreSQL's memory context system
- The function performs strict format validation with detailed error messages including the problematic token and expected length
- Handles architecture-dependent Datum sizes correctly by using sizeof(Datum)
- Part of the broader node serialization/deserialization infrastructure essential for prepared statements and plan caching