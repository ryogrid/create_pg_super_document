# pg_relation_is_updatable

## Location
[src/backend/utils/adt/misc.c:648-664](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/misc.c#L648-L664)

## Overview
pg_relation_is_updatable is a SQL-callable function that determines which update operations (INSERT, UPDATE, DELETE) are supported by a specified relation.

## Definition


## Detailed Description
This function serves as a SQL interface to PostgreSQL's internal updatability checking mechanism. It takes a relation OID and a boolean flag indicating whether to include trigger-based updatability, then delegates to the internal  function in rewriteHandler.c to perform the actual analysis.

The function returns an integer bitmask indicating which types of update operations are permitted on the relation. This includes checking for various constraints, permissions, and structural properties that might prevent updates, inserts, or deletes.

The  parameter controls whether the analysis should consider triggers that might make an otherwise non-updatable relation updatable through INSTEAD OF triggers.

## Parameters / Member Variables
-  (Oid): The OID of the relation to check for updatability
-  (bool): Whether to consider triggers when determining updatability

## Dependencies
- Functions called/Symbols referenced:
  -  (to extract the relation OID argument)
  -  (to extract the include_triggers boolean argument)
  -  (the core function that performs updatability analysis)
  -  (to return the result as a 32-bit integer)
  -  (empty list constant, passed as the columns parameter)
- Called from:
  - No direct callers found in the codebase (SQL-callable function)

## Notes and Other Information
- Located in src/backend/utils/adt/misc.c:648-664
- This function is part of PostgreSQL's SQL API for relation introspection
- The actual updatability logic is implemented in  in rewriteHandler.c
- Returns a bitmask where different bits indicate support for INSERT, UPDATE, and DELETE operations
- Used by applications and tools that need to determine what operations are possible on a relation
- The function passes NULL as the fourth parameter to , which means it's not checking specific column updatability
- The NIL parameter indicates that all columns are being considered rather than a specific subset