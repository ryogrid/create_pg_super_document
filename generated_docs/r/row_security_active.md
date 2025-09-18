# row_security_active

## Location
src/backend/utils/misc/rls.c: 142 - 152

## Overview
A SQL-callable function that checks whether Row Level Security is active for a given table OID.

## Definition


## Detailed Description
This function serves as a SQL-accessible wrapper around the  function. It takes a table OID as input and returns a boolean value indicating whether RLS is currently active for that table. Unlike the underlying  function, this function treats  and  as equivalent (both return false), only returning true when RLS is definitively enabled ().

The function calls  with  for the checkAsUser parameter (meaning it checks the current user) and  for the noError parameter (so it won't throw errors if RLS bypass is attempted).

## Parameters / Member Variables
- Takes a single argument via :
  -  (OID): The object identifier of the table to check

## Dependencies
- Functions called/Symbols referenced:
  - check_enable_rls
  - RLS_ENABLED
- Called from (representative examples):
  - No direct references found (likely called from SQL queries)

## Notes and Other Information
- This function is designed to be called from SQL as a system function
- Returns a simple boolean result, making RLS status easily accessible to SQL queries
- Uses the noError=true parameter to avoid throwing exceptions during the check
- The function simplifies the three-state return value of check_enable_rls into a binary active/inactive result
- Part of PostgreSQL's system catalog functions for introspecting security settings