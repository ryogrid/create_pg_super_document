# row_security_active_name

## Location
[src/backend/utils/misc/rls.c:153-167](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/rls.c#L153-L167)

## Overview
A SQL-callable function that checks whether Row Level Security is active for a table specified by its qualified name.

## Definition


## Detailed Description
This function provides the same functionality as  but accepts a table name (as text) instead of a table OID. It takes a qualified table name, resolves it to its corresponding OID, and then calls  to determine if RLS is active. Like , it returns a boolean value indicating whether RLS is definitively enabled for the specified table.

The function performs name resolution by:
1. Converting the input text to a qualified name list
2. Creating a RangeVar from the name list  
3. Resolving the RangeVar to get the table OID
4. Calling check_enable_rls with the resolved OID

The function uses NoLock when resolving the table name since it might not have the necessary privileges to lock the table.

## Parameters / Member Variables
- Takes a single argument via :
  -  (text): The qualified name of the table to check (e.g., 'schema.table' or just 'table')

## Dependencies
- Functions called/Symbols referenced:
  - [makeRangeVarFromNameList](../m/makeRangeVarFromNameList.md)
  - textToQualifiedNameList
  - RangeVarGetRelid
  - [check_enable_rls](../c/check_enable_rls.md)
  - [RangeVar](../R/RangeVar.md)
  - RLS_ENABLED
- Called from (representative examples):
  - No direct references found (likely called from SQL queries)

## Notes and Other Information
- This function is designed to be called from SQL as a system function, providing a name-based interface
- More user-friendly than the OID-based version since users typically know table names rather than OIDs
- Uses NoLock when resolving table names to avoid privilege-related issues
- Part of PostgreSQL's system catalog functions for introspecting security settings
- The qualified name can include schema specification (e.g., 'public.mytable') or use the current search path
- Returns the same simplified boolean result as row_security_active (active/inactive rather than the three-state result of check_enable_rls)