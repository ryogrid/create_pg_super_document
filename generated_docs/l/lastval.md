# lastval

## Location
src/backend/commands/sequence.c: 897 - 944

## Overview
Returns the value most recently obtained by nextval() in the current session, without requiring a specific sequence identifier.

## Definition


## Detailed Description
The lastval function returns the value that was most recently returned by nextval() in the current session, regardless of which sequence it came from. This is a session-level function that tracks the globally last-used sequence via the last_used_seq static variable. Unlike currval(), it doesn't require specifying a sequence identifier, making it convenient for applications that work with a single sequence or want the most recent value from any sequence.

The function performs several safety checks: it verifies that nextval() has been called in the session, checks that the sequence still exists (in case it was dropped), and validates permissions on the sequence. It uses the last_used_seq global variable to track which sequence was most recently accessed.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro (no specific arguments for this function)

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCacheExists1: Check if the sequence still exists in the system catalog
  - [lock_and_open_sequence](lock_and_open_sequence.md): Open and lock the sequence relation
  - [pg_class_aclcheck](../p/pg_class_aclcheck.md): Check access permissions on the sequence
  - ACL_SELECT: Permission flag for SELECT operations
  - ACL_USAGE: Permission flag for USAGE operations
  - [sequence_close](../s/sequence_close.md): Close and unlock the sequence relation
  - PG_RETURN_INT64: Return the 64-bit integer result
- Called from (representative examples):
  - No direct references found (called via SQL function dispatch)

## Notes and Other Information
- Requires that nextval() has been called on at least one sequence in the current session
- Uses a global last_used_seq variable to track the most recently accessed sequence
- Performs permission checks requiring either SELECT or USAGE privileges on the tracked sequence
- Will error if the tracked sequence has been dropped since the last nextval() call
- More convenient than currval() when working with a single sequence or when the specific sequence OID is not readily available
- Part of PostgreSQL's sequence management system in src/backend/commands/sequence.c:897