# DatumGetCString

## Location
[src/include/postgres.h:335-349](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/postgres.h#L335-L349)

## Overview
Converts a Datum value to a C string (null-terminated string), providing specialized extraction of string data from PostgreSQL's generic Datum representation.

## Definition
```c
static inline char *DatumGetCString(Datum X)
```

## Detailed Description
DatumGetCString is a static inline function that extracts a C string from a Datum by casting the result of DatumGetPointer to a char pointer. This function is specifically designed for handling string data in PostgreSQL's type system, where C strings are not full-fledged PostgreSQL types but are commonly used as input to type input functions. The function assumes the Datum contains a pointer to a null-terminated string and provides a convenient, type-safe way to access it.

## Parameters / Member Variables
- `X`: The input Datum value that contains a pointer to a null-terminated C string

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetPointer](DatumGetPointer.md) (called internally)
- Called from (representative examples):
  - [brin_range_serialize](../b/brin_range_serialize.md)
  - [fill_val](../f/fill_val.md)
  - [ExecInterpExpr](../E/ExecInterpExpr.md)
  - [coerce_type](../c/coerce_type.md)
  - [OutputFunctionCall](../O/OutputFunctionCall.md)
  - PG_GETARG_CSTRING

## Notes and Other Information
- This is a static inline function defined in postgres.h, making it available throughout the PostgreSQL codebase
- Internally uses DatumGetPointer and casts the result to char*, providing a specialized interface for string data
- Extensively used throughout PostgreSQL for handling string inputs, particularly in type input functions and data conversion routines
- The function performs no validation on the string content or null-termination - it assumes the Datum contains a valid C string pointer
- C strings are not full PostgreSQL types but serve as an important interface mechanism for the type system
- Widely used in JSON processing, formatting functions, type coercion, and procedural language interfaces