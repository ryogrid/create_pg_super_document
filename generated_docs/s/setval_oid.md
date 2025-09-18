# setval_oid

## Location
src/backend/commands/sequence.c: 1049 - 1063

## Overview
Implements the 2-argument form of setval(), setting a sequence to a specific value with the assumption that it has been called.

## Definition


## Detailed Description
The setval_oid function provides the PostgreSQL function interface for the 2-argument form of setval(sequence_oid, value). This is a thin wrapper around the internal do_setval function that automatically sets the iscalled parameter to true, meaning the sequence is marked as having been called. This implies that the next call to nextval() will return the set value plus the sequence's increment.

This function is commonly used in applications and pg_dump operations where you want to set a sequence to a specific value and have subsequent nextval() calls continue from there. It requires UPDATE privileges on the target sequence.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro containing:
  -  (Oid): The object identifier of the sequence relation to modify
  -  (int64): The value to set as the sequence's current position

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_OID: Extract OID argument from function call
  - PG_GETARG_INT64: Extract 64-bit integer argument from function call
  - [do_setval](../d/do_setval.md): Internal function that performs the actual setval operation
  - PG_RETURN_INT64: Return the set value as result
- Called from (representative examples):
  - No direct references found (called via SQL function dispatch)

## Notes and Other Information
- This is the standard 2-argument setval() interface that most applications use
- Always sets iscalled=true, meaning nextval() will increment from the set value
- Returns the value that was set, allowing for convenient usage in expressions
- Requires UPDATE permission on the sequence (checked by do_setval)
- For cases where you need to control the iscalled flag explicitly, use setval3_oid instead
- Part of PostgreSQL's sequence management system in src/backend/commands/sequence.c:1049