# brin_minmax_multi_distance_pg_lsn

## Location
[src/backend/access/brin/brin_minmax_multi.c:2191-2211](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_minmax_multi.c#L2191-L2211)

## Overview
Computes the distance between two PostgreSQL Log Sequence Number (LSN) values by directly subtracting their underlying int64 representations, used by BRIN minmax multi operator classes for pg_lsn data types.

## Definition
```c
Datum brin_minmax_multi_distance_pg_lsn(PG_FUNCTION_ARGS)
```

## Detailed Description
This function calculates the numerical distance between two pg_lsn (Log Sequence Number) values, which represent positions in the PostgreSQL Write-Ahead Logging (WAL) stream. Since LSN values are internally stored as int64 encoding positions in the WAL stream, the distance calculation is straightforward arithmetic subtraction. This function is part of the BRIN minmax multi operator class infrastructure, enabling efficient indexing of pg_lsn columns by maintaining multiple min/max pairs per block range.

## Parameters / Member Variables
- `PG_GETARG_LSN(0)`: The first LSN value (lsna)
- `PG_GETARG_LSN(1)`: The second LSN value (lsnb)  
- Returns: `float8` representing the byte distance between the two LSN positions

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_LSN (macro for extracting LSN arguments)
  - PG_RETURN_FLOAT8 (macro for returning float8 result)
  - XLogRecPtr (typedef for LSN, internally uint64)
- Called from (representative examples):
  - No direct references found in codebase

## Notes and Other Information
- The function assumes lsnb >= lsna and includes an Assert to verify this condition
- LSN values represent byte offsets in the WAL stream, making direct subtraction meaningful
- The result represents the number of bytes of WAL data between the two LSN positions
- This function is typically registered in BRIN operator class definitions for pg_lsn columns
- LSN distance calculations are crucial for replication lag monitoring and WAL-based operations
- The distance is cast to float8 for consistency with other BRIN distance functions

## Simplified Source

```c
Datum brin_minmax_multi_distance_pg_lsn(PG_FUNCTION_ARGS) {
    // Extract the two LSN values
    XLogRecPtr lsna = PG_GETARG_LSN(0);
    XLogRecPtr lsnb = PG_GETARG_LSN(1);

    // Calculate distance as simple subtraction (LSN is just int64 position)
    float8 delta = (lsnb - lsna);

    return PG_RETURN_FLOAT8(delta);
}
```