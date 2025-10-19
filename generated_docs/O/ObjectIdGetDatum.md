# ObjectIdGetDatum

## Location
[src/include/postgres.h:252-261](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/postgres.h#L252-L261)

## Overview
ObjectIdGetDatum is a static inline function that converts an object identifier (Oid) value to its Datum representation, serving as a type conversion utility in PostgreSQL's internal data representation system.

## Definition
static inline Datum ObjectIdGetDatum(Oid X)

## Detailed Description
ObjectIdGetDatum performs a simple type cast from an Oid to a Datum. This function is the inverse of DatumGetObjectId and is part of PostgreSQL's datum conversion interface. It provides a consistent method for converting typed Oid values into the generic Datum representation used throughout the PostgreSQL function call interface. The function performs no validation or transformation - it simply casts the input Oid directly to a Datum type. This is a zero-cost abstraction that enhances code readability and maintains consistency in the type conversion API.

## Parameters / Member Variables
- X: An Oid (Object Identifier) value to be converted to Datum representation

## Dependencies
- Functions called/Symbols referenced:
  - (None - performs direct cast)
- Called from (representative examples):
  - (No direct references found in current codebase analysis)

## Notes and Other Information
- This is a static inline function defined in src/include/postgres.h, making it available throughout the codebase
- Part of the family of *GetDatum conversion functions that provide type-safe conversion to Datum values
- Complementary to DatumGetObjectId, forming a bidirectional conversion pair
- The function assumes the input Oid is valid - no validation is performed
- May be used in contexts where OID values need to be passed through the PostgreSQL function call interface
- Currently appears to have limited usage in the analyzed codebase, possibly indicating it's provided for API completeness or used in extensions

## Simplified Source

```c
static inline Datum ObjectIdGetDatum(Oid X) {
    // Simple cast: convert Object ID to Datum format
    return (Datum) X;
}
```