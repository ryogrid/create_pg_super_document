# DatumGetCommandId

## Location
[src/include/postgres.h:292-301](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/postgres.h#L292-L301)

## Overview
Converts a Datum value to a CommandId type, providing type-safe extraction of command identifier values from PostgreSQL's generic Datum representation.

## Definition
```c
static inline CommandId DatumGetCommandId(Datum X)
```

## Detailed Description
DatumGetCommandId is a static inline function that performs a simple type cast from Datum to CommandId. This function is part of PostgreSQL's type conversion system that allows safe extraction of specific data types from the generic Datum container. Command identifiers are used internally by PostgreSQL to track command execution within transactions, and this function provides the standard way to extract CommandId values from Datum representations when needed by the system's internal operations.

## Parameters / Member Variables
- `X`: The input Datum value that contains a command identifier to be extracted

## Dependencies
- Functions called/Symbols referenced:
  - CommandId (type)
- Called from (representative examples):
  - PG_GETARG_COMMANDID

## Notes and Other Information
- This is a static inline function defined in postgres.h, making it available throughout the PostgreSQL codebase
- The function performs a simple cast with no validation, assuming the input Datum actually contains a CommandId value
- CommandId is typically used for tracking command execution within transactions
- Part of PostgreSQL's broader Datum conversion API that provides type-safe extraction methods

## Simplified Source

```c
static inline CommandId DatumGetCommandId(Datum X) {
    // Simple cast: extract CommandId from Datum
    return (CommandId) X;
}
```