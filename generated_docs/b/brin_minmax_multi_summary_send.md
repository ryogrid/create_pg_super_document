# brin_minmax_multi_summary_send

## Location
[src/backend/access/brin/brin_minmax_multi.c:3134-3137](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_minmax_multi.c#L3134-L3137)

## Overview
This function serves as the binary output routine for the BRIN minmax-multi summary type, delegating to the standard bytea send function since the summary data is internally stored as bytea.

## Definition
```c
Datum brin_minmax_multi_summary_send(PG_FUNCTION_ARGS)
```

## Detailed Description
The `brin_minmax_multi_summary_send` function handles binary output (serialization) operations for the `brin_minmax_multi_summary` data type. Since BRIN minmax-multi summaries are internally stored as bytea (byte array) data, this function simply delegates to PostgreSQL's standard `byteasend` function to handle the binary serialization.

This approach leverages the existing bytea serialization infrastructure rather than implementing custom binary output logic, which is efficient and maintains consistency with PostgreSQL's data type system.

## Parameters / Member Variables
This function follows the PostgreSQL function calling convention using `PG_FUNCTION_ARGS`:
- `fcinfo`: Function call information structure containing the BRIN minmax-multi summary to be serialized

## Dependencies
- Functions called/Symbols referenced:
  - [byteasend](byteasend.md) (PostgreSQL's standard bytea binary output function)
- Called from (representative examples):
  - No direct references found (likely called via PostgreSQL's type system during binary output operations)

## Notes and Other Information
- This function is part of PostgreSQL's type system interface for binary I/O operations
- The delegation to `byteasend` indicates that BRIN minmax-multi summaries use the same binary format as bytea
- This contrasts with `brin_minmax_multi_summary_recv` which explicitly prevents binary input operations
- The asymmetry (send allowed, recv forbidden) suggests that these summaries can be serialized for storage/transmission but should not be created from external binary input
- Located in src/backend/access/brin/brin_minmax_multi.c:3134-3137