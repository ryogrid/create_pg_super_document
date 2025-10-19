# void_recv

## Location
[src/backend/utils/adt/pseudotypes.c:275-284](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pseudotypes.c#L275-L284)

## Overview
The void_recv function is an input function for the void pseudo-type that handles receiving binary data for void values during PostgreSQL's binary protocol communication.

## Definition

```c
Datum
void_recv(PG_FUNCTION_ARGS)
```
## Detailed Description
The void_recv function is responsible for deserializing binary input for the void pseudo-type in PostgreSQL. The function serves as a binary input handler that consumes no bytes from the input stream and returns a void datum. The implementation includes a comment noting that any attempt to send non-empty data will result in an "invalid message format" error, ensuring that only empty strings are accepted as valid input for void types during binary protocol operations.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - PG_RETURN_VOID (macro for returning void datum)
- Called from (representative examples):
  - (No direct references found in codebase)

## Notes and Other Information
- Part of PostgreSQL's pseudo-type system located in src/backend/utils/adt/pseudotypes.c
- Enforces that void type binary input must be empty to maintain type system integrity
- Works in conjunction with binary protocol serialization/deserialization infrastructure
- The function's strict validation prevents malformed binary data from being accepted as void values

## Simplified Source

```c
Datum void_recv(PG_FUNCTION_ARGS) {
    // Binary input for void type - consumes no bytes
    // Sending non-empty data will cause "invalid message format" error
    PG_RETURN_VOID();
}
```