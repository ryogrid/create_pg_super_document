# byteasend

## Location
[src/backend/utils/adt/varlena.c:490-497](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L490-L497)

## Overview
Converts bytea data to binary format for transmission over the PostgreSQL wire protocol. This is a simple pass-through function that copies the input bytea.

## Definition
```c
Datum byteasend(PG_FUNCTION_ARGS)
```

## Detailed Description
The byteasend function is the binary output function for the bytea data type in PostgreSQL's type input/output system. Unlike text output functions that must convert data to string representations, byteasend handles the case where the internal representation (bytea) is already in the desired binary format.

As noted in the source comment, this is a "special case" where the function simply copies the input bytea data. The bytea type stores binary data directly, so no conversion is necessary when sending it in binary format over the wire protocol. The function uses PG_GETARG_BYTEA_P_COPY to ensure it gets a copy of the data that it can safely return.

## Parameters / Member Variables
- Input: bytea value retrieved via `PG_GETARG_BYTEA_P_COPY(0)` - the binary data to be sent
- Returns: bytea structure via `PG_RETURN_BYTEA_P()`

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BYTEA_P_COPY
  - PG_RETURN_BYTEA_P
- Called from:
  - [brin_bloom_summary_send](brin_bloom_summary_send.md) (BRIN bloom index)
  - [brin_minmax_multi_summary_send](brin_minmax_multi_summary_send.md) (BRIN minmax-multi index)
  - [pg_dependencies_send](../p/pg_dependencies_send.md) (statistics dependencies)
  - [pg_mcv_list_send](../p/pg_mcv_list_send.md) (statistics MCV lists)
  - [pg_ndistinct_send](../p/pg_ndistinct_send.md) (statistics ndistinct)

## Notes and Other Information
- This function is the counterpart to bytearecv for binary protocol communication
- Uses PG_GETARG_BYTEA_P_COPY to ensure memory safety by getting a copy of the input
- The simplicity of this function reflects that bytea's internal format is already suitable for binary transmission
- Used by various PostgreSQL subsystems that need to send binary data, particularly statistics and indexing components
- Part of PostgreSQL's type system infrastructure for handling binary format I/O operations