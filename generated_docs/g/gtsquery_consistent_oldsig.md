# gtsquery_consistent_oldsig

## Location
[src/backend/utils/adt/tsquery_gist.c:273-276](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsquery_gist.c#L273-L276)

## Overview
A backward compatibility wrapper function that provides support for reloading pre-9.6 contrib/tsearch2 opclass declarations by maintaining the old function signature for gtsquery_consistent.

## Definition

```c
Datum
gtsquery_consistent_oldsig(PG_FUNCTION_ARGS)
```
## Detailed Description
The gtsquery_consistent_oldsig function serves as a compatibility shim for PostgreSQL's TSQuery GiST index operations. This function was introduced to maintain backward compatibility with pre-PostgreSQL 9.6 contrib/tsearch2 operator class declarations.

Originally, gtsquery_consistent was declared in pg_proc.h with function arguments that did not match the documented conventions for GiST support functions. When PostgreSQL fixed the function signature to properly conform to GiST conventions, this compatibility function was created to ensure that existing tsearch2 installations could still function without requiring manual intervention.

The function simply delegates all calls to the current gtsquery_consistent function, allowing old opclass declarations to continue working while using the updated implementation internally.

## Parameters / Member Variables
- Uses PostgreSQL's PG_FUNCTION_ARGS macro for function arguments:

## Dependencies
- Functions called/Symbols referenced:
  - [gtsquery_consistent](gtsquery_consistent.md) (delegates all functionality to this function)
  - fcinfo (PostgreSQL function call info, passed through to gtsquery_consistent)
- Called from:
  - No direct references found (likely called through GiST function table for legacy opclass declarations)

## Notes and Other Information
- This is a temporary compatibility function intended for eventual removal
- Part of PostgreSQL's approach to maintaining backward compatibility during major version upgrades
- Only exists to support reloading of pre-9.6 contrib/tsearch2 operator class declarations
- The function performs no processing itself, acting purely as a delegation wrapper
- Maintains the fmgr interface convention like other PostgreSQL functions
- Located in src/backend/utils/adt/tsquery_gist.c:273-276
- Should be removed in future PostgreSQL versions when pre-9.6 compatibility is no longer needed