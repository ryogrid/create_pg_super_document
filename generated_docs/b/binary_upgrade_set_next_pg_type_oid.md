# binary_upgrade_set_next_pg_type_oid

## Location
src/backend/utils/adt/pg_upgrade_support.c: 54 - 64

## Overview
Sets the OID to be assigned to the next data type created during binary upgrade operations.

## Definition


## Detailed Description
This function is part of PostgreSQL's binary upgrade support system that allows pg_upgrade to control OID assignment during database upgrades to preserve object identities. The function accepts a type OID as input and stores it in the global variable , which will be used by the system when creating the next data type.

The function can only be called when the server is running in binary upgrade mode ( is true). This restriction ensures that OID manipulation is only allowed during controlled upgrade operations and not during normal database operations. This is critical for maintaining data type consistency during major version upgrades.

## Parameters / Member Variables
-  (PG_GETARG_OID(0)): The OID value to assign to the next data type that will be created

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
- Essential for preserving user-defined and system type OIDs during major version upgrades
- Works in conjunction with type creation functions that check this variable to assign the specified OID