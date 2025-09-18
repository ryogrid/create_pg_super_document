# DatumGetJsonPathPCopy

## Location
src/include/utils/jsonpath.h: 41 - 45

## Overview
DatumGetJsonPathPCopy is an inline function that converts a PostgreSQL Datum to a JsonPath pointer by detoasting the datum and creating a modifiable copy.

## Definition
```c
static inline JsonPath *
DatumGetJsonPathPCopy(Datum d)
{
    return (JsonPath *) PG_DETOAST_DATUM_COPY(d);
}
```

## Detailed Description
DatumGetJsonPathPCopy is a type conversion utility function that safely extracts a JsonPath structure from a PostgreSQL Datum while creating a copy that can be safely modified. The function uses the PG_DETOAST_DATUM_COPY macro to handle the conversion, which will decompress and/or extract the data from PostgreSQL's TOAST (The Oversized-Attribute Storage Technique) storage if necessary, and always creates a copy in the current memory context.

Unlike DatumGetJsonPathP, this function guarantees that the returned JsonPath structure is a separate copy that can be modified without affecting the original data. This is essential when the JsonPath needs to be altered or when the lifetime of the returned pointer needs to extend beyond the original datum's scope.

## Parameters / Member Variables
- `d`: The input Datum containing a JsonPath value that may be stored in TOAST format

## Dependencies
- Functions called/Symbols referenced:
  - PG_DETOAST_DATUM_COPY (macro for detoasting and copying PostgreSQL data)
  - JsonPath (structure type definition)
- Called from (representative examples):
  - PG_GETARG_JSONPATH_P_COPY (macro wrapper in src/include/utils/jsonpath.h:47)

## Notes and Other Information
- This is a static inline function defined in the header file for performance
- The function always creates a copy of the data, making it safe for modification
- The copy is allocated in the current memory context and will be automatically freed when the context is reset
- Use this function when you need to modify the JsonPath structure or when the returned pointer needs to outlive the original datum
- The PG_GETARG_JSONPATH_P_COPY macro is the typical way this function is used in PostgreSQL function implementations
- More expensive than DatumGetJsonPathP due to the copy operation, so only use when modification is required