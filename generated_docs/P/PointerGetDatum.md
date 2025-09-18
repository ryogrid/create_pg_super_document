# PointerGetDatum

## Location
[src/include/postgres.h:322-334](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/postgres.h#L322-L334)

## Overview
Converts a pointer value to a Datum type, providing the inverse operation of DatumGetPointer for storing pointer values in PostgreSQL's generic Datum representation.

## Definition
```c
static inline Datum PointerGetDatum(const void *X)
```

## Detailed Description
PointerGetDatum is a static inline function that performs a simple type cast from a const void pointer to Datum. This function is the complementary operation to DatumGetPointer, allowing pointer values to be stored in PostgreSQL's generic Datum container. This conversion is essential when working with variable-length data structures or complex objects that must be passed through PostgreSQL's function interface, which uses Datum as the universal parameter and return type.

## Parameters / Member Variables
- `X`: The input const void pointer to be converted to Datum representation

## Dependencies
- Functions called/Symbols referenced:
  - Datum (type)
- Called from (representative examples):
  - (No direct references found in the current analysis)

## Notes and Other Information
- This is a static inline function defined in postgres.h, making it available throughout the PostgreSQL codebase
- The function performs a simple cast with no validation, directly converting the pointer to Datum
- Takes a const void pointer parameter, allowing conversion of any pointer type without requiring explicit casting
- Part of PostgreSQL's broader Datum conversion API that provides type-safe conversion methods between specific types and the generic Datum representation
- While no direct references were found in the current analysis, this function is fundamental for the PostgreSQL type system and likely used extensively through macros or indirect calls