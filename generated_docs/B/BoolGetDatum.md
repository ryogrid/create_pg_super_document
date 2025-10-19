# BoolGetDatum

## Location
[src/include/postgres.h:102-111](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/postgres.h#L102-L111)

## Overview
BoolGetDatum is an inline function that converts a boolean value to a PostgreSQL Datum representation.

## Definition
static inline Datum BoolGetDatum(bool X)

## Detailed Description
BoolGetDatum is the complement to DatumGetBool, providing conversion from C boolean values to PostgreSQL Datum format. The function converts a boolean parameter to a Datum by returning 1 for true values and 0 for false values. This standardizes the boolean representation within PostgreSQLs datum system, ensuring consistent boolean storage and manipulation. The function is implemented as a static inline function for optimal performance in frequently used boolean operations.

## Parameters / Member Variables
- `X`: A C boolean value to be converted to Datum format. True values become 1, false values become 0.

## Dependencies
- Functions called/Symbols referenced:
  - None (simple conditional assignment)
- Called from (representative examples):
  - [Boolean](Boolean.md) type output functions
  - Functions returning boolean results as Datums
  - SQL function implementations returning boolean values

## Notes and Other Information
- This is a static inline function defined in src/include/postgres.h for maximum performance
- Forms a pair with DatumGetBool for bidirectional boolean/Datum conversion
- Uses the standard PostgreSQL convention of representing true as 1 and false as 0
- Part of the fundamental datum conversion system that enables type-safe value handling
- The implementation ensures consistent boolean representation across the PostgreSQL system

## Simplified Source

```c
static inline Datum
BoolGetDatum(bool X)
{
    // Convert boolean to Datum: true becomes 1, false becomes 0
    return (Datum) (X ? 1 : 0);
}
```