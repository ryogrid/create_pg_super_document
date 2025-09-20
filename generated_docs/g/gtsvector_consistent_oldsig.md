# gtsvector_consistent_oldsig

## Location
[src/backend/utils/adt/tsgistidx.c:803-808](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsgistidx.c#L803-L808)

## Overview
A backward compatibility wrapper function that maintains support for pre-9.6 contrib/tsearch2 opclass declarations by providing the old function signature.

## Definition

```c
Datum
gtsvector_consistent_oldsig(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as a compatibility shim for PostgreSQL installations that may have older opclass declarations from contrib/tsearch2 extensions created before version 9.6. The original gtsvector_consistent function had its signature modified to match PostgreSQL's documented conventions for GiST support functions, but existing opclass definitions still referenced the old signature.

The function is essentially a pass-through wrapper that simply forwards all arguments to the current gtsvector_consistent implementation, ensuring that old installations continue to work without requiring manual opclass recreation.

This compatibility layer allows for seamless upgrades while maintaining backward compatibility with existing text search indexes that were created with older versions of the tsearch2 extension.

## Parameters / Member Variables
- : Function call info structure containing all arguments (passed through transparently)
- Returns: Result from gtsvector_consistent function call

## Dependencies
- Functions called/Symbols referenced:
  - : The current implementation of the GiST consistent function for tsvector
- Called from (representative examples):
  - Legacy GiST opclass definitions from pre-9.6 tsearch2 extensions

## Notes and Other Information
- File location: src/backend/utils/adt/tsgistidx.c:803-808
- This is a temporary compatibility measure intended to be removed in future PostgreSQL versions
- The function exists solely to maintain backward compatibility during PostgreSQL upgrades
- No functional logic is implemented here - it's purely a forwarding wrapper
- Users with old tsearch2 installations should eventually recreate their opclasses to use the proper signature