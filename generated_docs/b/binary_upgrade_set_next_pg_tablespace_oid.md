# binary_upgrade_set_next_pg_tablespace_oid

## Location
[src/backend/utils/adt/pg_upgrade_support.c:43-53](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_upgrade_support.c#L43-L53)

## Overview
Sets the OID to be assigned to the next tablespace created during binary upgrade operations.

## Definition


## Detailed Description
This function is part of PostgreSQL's binary upgrade support system, which allows pg_upgrade to control OID assignment during database upgrades to preserve object identities. The function accepts a tablespace OID as input and stores it in the global variable , which will be used by the system when creating the next tablespace.

The function can only be called when the server is running in binary upgrade mode ( is true). This restriction ensures that OID manipulation is only allowed during controlled upgrade operations and not during normal database operations.

## Parameters / Member Variables
-  (PG_GETARG_OID(0)): The OID value to assign to the next tablespace that will be created

## Dependencies
- Functions called/Symbols referenced:
  -  (macro that validates binary upgrade mode)
  -  (PostgreSQL function return macro)
- Global variable modified:
  -  (declared in )
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL function interface)

## Notes and Other Information
- This function is exposed as a SQL-callable function for use by pg_upgrade tools
- The function performs a security check via  macro which throws an error if not in binary upgrade mode
- The global variable  is declared as  in , making it accessible across PostgreSQL modules
- Located in 
- Part of the broader binary upgrade infrastructure that preserves object OIDs during major version upgrades