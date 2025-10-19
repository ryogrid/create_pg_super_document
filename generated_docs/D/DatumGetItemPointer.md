# DatumGetItemPointer

## Location
[src/include/storage/itemptr.h:231-236](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/itemptr.h#L231-L236)

## Overview
Converts a PostgreSQL Datum value to an ItemPointer, used in the function call interface for extracting ItemPointer arguments.

## Definition
```c
static inline ItemPointer
DatumGetItemPointer(Datum X)
```

## Detailed Description
This function provides a type-safe conversion from a Datum (PostgreSQL's generic data type) to an ItemPointer. It is part of PostgreSQL's datum conversion system that enables the generic function call interface to work with specific data types. The function simply casts a Datum to an ItemPointer by first extracting the pointer value using DatumGetPointer and then casting it to ItemPointer type. This is used internally by the PostgreSQL function call protocol when ItemPointer values need to be extracted from function arguments.

## Parameters / Member Variables
- `X`: Datum value to convert to ItemPointer

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetPointer](DatumGetPointer.md) (from postgres.h)
- Called from (representative examples):
  - PG_GETARG_ITEMPOINTER (macro for getting ItemPointer function arguments)

## Notes and Other Information
- This is a static inline function for performance efficiency
- Part of PostgreSQL's type conversion system for the function call interface
- Used primarily in conjunction with PG_GETARG_ITEMPOINTER macro to extract ItemPointer arguments from PostgreSQL function calls
- The conversion assumes that the Datum actually contains a valid ItemPointer - no type checking is performed
- Counterpart to ItemPointerGetDatum which performs the reverse conversion

## Simplified Source

```c
static inline ItemPointer DatumGetItemPointer(Datum X) {
    // Convert Datum to ItemPointer by extracting pointer value
    return (ItemPointer) DatumGetPointer(X);
}
```