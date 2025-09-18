# binary_upgrade_set_next_multirange_array_pg_type_oid

## Location
[src/backend/utils/adt/pg_upgrade_support.c:87-97](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_upgrade_support.c#L87-L97)

## Overview
Sets the OID to be assigned to the next multirange array type created during binary upgrade operations.

## Definition


## Detailed Description
This function is part of PostgreSQL's binary upgrade support system that allows pg_upgrade to control OID assignment during database upgrades to preserve object identities. The function accepts a multirange array type OID as input and stores it in the global variable , which will be used by the system when creating the next multirange array type.

This function represents the final piece in the complete range type family hierarchy. When a range type is created in PostgreSQL, the system automatically generates four related types: the base range type, its array type, a multirange type, and a multirange array type. During binary upgrades, all four types must have their OIDs preserved to maintain referential integrity and ensure that existing dependencies continue to work correctly.

The function can only be called when the server is running in binary upgrade mode ( is true). This restriction ensures that OID manipulation is only allowed during controlled upgrade operations.

## Parameters / Member Variables
-  (PG_GETARG_OID(0)): The OID value to assign to the next multirange array type that will be created

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
- Completes the quartet of functions needed for comprehensive range type family OID control during upgrades
- Works in conjunction with multirange array type creation functions that check this variable to assign the specified OID
- The variable name uses 'mrng' as abbreviation for 'multirange' to maintain consistency with PostgreSQL naming conventions
- Essential for maintaining the complete type dependency chain: range → array, multirange → multirange array
- Critical for upgrades involving databases with custom range types and their complete type ecosystems