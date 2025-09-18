# ItemPointerGetDatum

## Location
src/include/storage/itemptr.h: 237 - 241

## Overview
Converts an ItemPointer to a PostgreSQL Datum value, used in the function call interface for returning ItemPointer values.

## Definition
```c
static inline Datum
ItemPointerGetDatum(const ItemPointerData *X)
```

## Detailed Description
This function provides a type-safe conversion from an ItemPointer to a Datum (PostgreSQL's generic data type). It is part of PostgreSQL's datum conversion system that enables the generic function call interface to work with specific data types. The function converts an ItemPointer to a Datum by using PointerGetDatum to create a Datum that contains the pointer value. This is used internally by the PostgreSQL function call protocol when ItemPointer values need to be returned from functions.

## Parameters / Member Variables
- `X`: Pointer to ItemPointerData structure to convert to Datum

## Dependencies
- Functions called/Symbols referenced:
  - PointerGetDatum (from postgres.h)
- Called from (representative examples):
  - PG_RETURN_ITEMPOINTER (macro for returning ItemPointer values from functions)

## Notes and Other Information
- This is a static inline function for performance efficiency
- Part of PostgreSQL's type conversion system for the function call interface
- Used primarily in conjunction with PG_RETURN_ITEMPOINTER macro to return ItemPointer values from PostgreSQL functions
- The conversion creates a Datum that contains a pointer to the ItemPointerData structure
- Counterpart to DatumGetItemPointer which performs the reverse conversion
- Essential for functions that need to return ItemPointer values through PostgreSQL's generic function interface