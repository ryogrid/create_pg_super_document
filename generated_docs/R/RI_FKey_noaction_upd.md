# RI_FKey_noaction_upd

## Location
[src/backend/utils/adt/ri_triggers.c:588-607](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ri_triggers.c#L588-L607)

## Overview
A trigger function that implements the NO ACTION referential integrity constraint behavior for UPDATE operations on the referenced table, rolling back the transaction if a foreign key constraint would be violated.

## Definition


## Detailed Description
This function is a PostgreSQL trigger function that enforces the NO ACTION referential integrity constraint when rows in a referenced (parent) table are updated. The NO ACTION constraint type means that if the update would result in orphaned foreign key references, the operation should be rejected and the current transaction rolled back.

The function performs validation to ensure it's called in the correct trigger context (UPDATE operation) and then delegates the actual constraint checking logic to the shared  function, which implements the core logic for both NO ACTION and RESTRICT constraint types.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro containing:
  - : Function call information structure containing trigger data and context

## Dependencies
- Functions called/Symbols referenced:
  - : Validates the trigger call context
  - : Shared implementation for restriction-based constraints
  - : Constant defining UPDATE trigger type
  - : Structure containing trigger execution context
  
- Called from (representative examples):
  - No direct callers found (invoked by PostgreSQL trigger system)

## Notes and Other Information
- This function is registered as a trigger function in the PostgreSQL system catalog
- The NO ACTION constraint type is functionally identical to RESTRICT in PostgreSQL's implementation
- The function is part of the referential integrity (RI) trigger system that maintains foreign key constraints
- Located in  at lines 588-607
- Returns a Datum value as required by PostgreSQL's function call interface