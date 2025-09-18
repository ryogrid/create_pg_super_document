# regprocout

## Location
[src/backend/utils/adt/regproc.c:136-197](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regproc.c#L136-L197)

## Overview
Converts a RegProcedure OID to its string representation, serving as the output function for the regproc data type with intelligent schema qualification.

## Definition


## Detailed Description
The regprocout function is the output conversion function for PostgreSQL's regproc data type. It takes a RegProcedure OID and converts it to a human-readable string representation. The function implements intelligent formatting logic:

1. **Invalid OID**: Returns "-" for InvalidOid (0)
2. **Valid procedure**: Returns the procedure name, with schema qualification only when necessary
3. **Unknown OID**: Returns the numeric OID string if the OID doesn't exist in pg_proc

The function's key feature is its schema qualification logic - it only includes the schema name when the procedure name would be ambiguous without it. This is determined by checking if regprocin would uniquely resolve the unqualified name back to the same OID.

## Parameters / Member Variables
- : Input RegProcedure OID to be converted to string representation

## Dependencies
- Functions called/Symbols referenced:
  - : Looks up procedure information in pg_proc system cache
  - : Extracts Form_pg_proc structure from HeapTuple
  - : Checks for bootstrap mode to skip namespace logic
  - : Tests if unqualified name would resolve uniquely
  - : Creates String node for function name
  - : Retrieves schema name for qualification
  - : Properly quotes and formats qualified names
  - : Releases system cache reference
  - : Returns the formatted string result
- Called from (representative examples):
  - No direct references found in the indexed codebase

## Notes and Other Information
- Bootstrap mode behavior: In bootstrap mode, always returns simple procedure name without schema qualification for debugging output
- Schema qualification intelligence: Only qualifies with schema name when the unqualified name would be ambiguous in the current search path
- Fallback handling: Returns numeric OID string for non-existent procedures rather than throwing errors
- Memory management: Uses pstrdup and palloc for result string allocation
- Cache usage: Leverages PostgreSQL's system cache for efficient pg_proc lookups