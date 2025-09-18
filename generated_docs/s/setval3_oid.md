# setval3_oid

## Location
[src/backend/commands/sequence.c:1064-1084](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/sequence.c#L1064-L1084)

## Overview
Implements the 3-argument form of setval(), allowing explicit control over both the sequence value and its 'called' state.

## Definition


## Detailed Description  
The setval3_oid function provides the PostgreSQL function interface for the 3-argument form of setval(sequence_oid, value, iscalled). This is a thin wrapper around the internal do_setval function that allows explicit control over the iscalled parameter. When iscalled=true, the sequence is marked as having been called, and nextval() will return the set value plus increment. When iscalled=false, nextval() will return the set value itself.

This function is primarily designed for pg_dump operations during database restoration, where exact sequence state recovery is critical. It allows pg_dump to restore sequences to their precise state, including whether they have been called or not. The 3-argument form is essential for data-only restores where sequences need to be reset to their exact pre-dump state.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro containing:
  -  (Oid): The object identifier of the sequence relation to modify
  -  (int64): The value to set as the sequence's current position
  -  (bool): Whether to mark the sequence as having been called

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_OID: Extract OID argument from function call
  - PG_GETARG_INT64: Extract 64-bit integer argument from function call
  - PG_GETARG_BOOL: Extract boolean argument from function call
  - [do_setval](../d/do_setval.md): Internal function that performs the actual setval operation
  - PG_RETURN_INT64: Return the set value as result
- Called from (representative examples):
  - No direct references found (called via SQL function dispatch)

## Notes and Other Information
- Primarily intended for pg_dump and database restoration operations
- The iscalled=false option is crucial for exact sequence state restoration
- May not work reliably if multiple users are concurrently accessing the sequence during restoration
- Requires UPDATE permission on the sequence (checked by do_setval)
- Returns the value that was set for consistency with the 2-argument form
- Provides the only mechanism to clear the is_called flag in an existing sequence
- Part of PostgreSQL's sequence management system in src/backend/commands/sequence.c:1064