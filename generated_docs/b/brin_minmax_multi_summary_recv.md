# brin_minmax_multi_summary_recv

## Location
src/backend/access/brin/brin_minmax_multi.c: 3117 - 3133

## Overview
This function serves as the binary input routine for the BRIN minmax-multi summary type, but intentionally raises an error to prevent binary input operations on this type.

## Definition
```c
Datum brin_minmax_multi_summary_recv(PG_FUNCTION_ARGS)
```

## Detailed Description
The `brin_minmax_multi_summary_recv` function is a stub implementation that explicitly prevents binary input operations on the `brin_minmax_multi_summary` data type. Instead of processing binary input data, it immediately raises a `FEATURE_NOT_SUPPORTED` error with an appropriate error message.

This design choice indicates that BRIN minmax-multi summary values are not intended to be created or received through binary input operations, likely because they are internal data structures that should only be created and manipulated through specific BRIN index operations.

## Parameters / Member Variables
This function follows the PostgreSQL function calling convention using `PG_FUNCTION_ARGS`:
- Input parameters: Expected binary input data (not processed due to intentional error)

## Dependencies
- Functions called/Symbols referenced:
  - ereport (for error reporting)
  - errcode (for error code specification)
  - errmsg (for error message formatting)
  - PG_RETURN_VOID
- Called from (representative examples):
  - No direct references found (likely called via PostgreSQL's type system when binary input is attempted)

## Notes and Other Information
- This function is part of PostgreSQL's type system interface requirements
- The intentional error prevents misuse of the brin_minmax_multi_summary type
- The ERRCODE_FEATURE_NOT_SUPPORTED error code indicates this is a deliberate design limitation
- The function includes a PG_RETURN_VOID() call to satisfy compiler requirements, though it's never reached due to the error
- Located in src/backend/access/brin/brin_minmax_multi.c:3117-3133