# currval_oid

## Location
[src/backend/commands/sequence.c:866-896](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/sequence.c#L866-L896)

## Overview
Returns the current value of a sequence identified by its OID, ensuring proper permissions and session validity.

## Definition

```c
Datum
currval_oid(PG_FUNCTION_ARGS)
```
## Detailed Description
The currval_oid function retrieves the current value of a sequence specified by its object identifier (OID). This function is the backend implementation for the SQL currval() function when called with a sequence OID. It performs security checks to ensure the user has appropriate permissions (SELECT or USAGE) on the sequence and validates that the sequence has been accessed in the current session via nextval() or setval().

The function maintains session-level state through the SeqTable structure, which tracks the last value returned by nextval() for each sequence in the current session. If currval() is called before nextval() or setval() in a session, it raises an error.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro containing:
  -  (Oid): The object identifier of the sequence relation

## Dependencies
- Functions called/Symbols referenced:
  - [init_sequence](../i/init_sequence.md): Initialize and lock the sequence relation
  - [pg_class_aclcheck](../p/pg_class_aclcheck.md): Check access permissions on the sequence
  - ACL_SELECT: Permission flag for SELECT operations
  - ACL_USAGE: Permission flag for USAGE operations  
  - [sequence_close](../s/sequence_close.md): Close and unlock the sequence relation
  - PG_RETURN_INT64: Return the 64-bit integer result
- Called from (representative examples):
  - No direct references found (called via SQL function dispatch)

## Notes and Other Information
- Requires that nextval() or setval() has been called on the sequence in the current session
- Performs permission checks requiring either SELECT or USAGE privileges
- Thread-safe through proper sequence locking via init_sequence()
- Returns the last value obtained by nextval(), not necessarily the actual current value in the sequence table
- Part of PostgreSQL's sequence management system in src/backend/commands/sequence.c:866