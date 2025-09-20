# pg_ndistinct_send

## Location
[src/backend/statistics/mvdistinct.c:408-424](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/mvdistinct.c#L408-L424)

## Overview
A PostgreSQL binary output function for the pg_ndistinct data type that delegates binary serialization to the standard bytea send routine.

## Definition

```c
Datum
pg_ndistinct_send(PG_FUNCTION_ARGS)
```
## Detailed Description
The pg_ndistinct_send function serves as the binary output routine for the pg_ndistinct data type in PostgreSQL. Since n-distinct statistics are internally stored and serialized as bytea values, this function simply delegates the binary output operation to the standard byteasend function. This approach leverages the existing bytea serialization infrastructure, ensuring consistent and efficient binary representation of n-distinct statistics data for network transmission or storage.

## Parameters / Member Variables
- Input parameter (via PG_FUNCTION_ARGS):

## Dependencies
- Functions called/Symbols referenced:
  - [byteasend](../b/byteasend.md) (standard PostgreSQL function for binary output of bytea data)
- Called from:
  - No direct references found (used as type send function)

## Notes and Other Information
- This function leverages PostgreSQL's existing bytea infrastructure for binary serialization
- The pg_ndistinct type is internally represented as bytea, making this delegation natural and efficient
- Used for binary protocol operations such as network transmission between client and server
- Located in src/backend/statistics/mvdistinct.c:408-424