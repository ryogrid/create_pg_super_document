# lappend_oid

## Location
src/backend/nodes/list.c: 375 - 392

## Overview
Appends an OID (Object Identifier) value to a PostgreSQL OidList data structure, returning a pointer to the modified list.

## Definition

```c
List *
lappend_oid(List *list, Oid datum)
```
## Detailed Description
The  function is a specialized version of  designed specifically for OID lists (T_OidList). It appends an OID value to the end of an OidList, handling both empty lists (NIL) and existing lists with elements. Like other lappend variants, this function may or may not destructively modify the original list structure, so callers must use the returned value rather than the original list pointer.

When the input list is NIL, the function creates a new OidList with a single OID element. For existing lists, it adds a new tail cell and stores the OID value. The function includes type assertions to ensure the list is specifically an OID list and performs invariant checking.

This function is extensively used throughout PostgreSQL for managing collections of object identifiers, including relation OIDs, type OIDs, function OIDs, and other database object references.

## Parameters / Member Variables
- : The OidList to append to, or NIL to create a new OID list
- : The OID value to be appended to the list

## Dependencies
- Functions called/Symbols referenced:
  - IsOidList (assertion check for OID list type)
  - new_list (creates new list when input is NIL, with T_OidList type)
  - new_tail_cell (adds new cell to existing list)
  - llast_oid (macro to access last OID element of list)
  - check_list_invariants (debugging/validation function)
- Called from (representative examples):
  - ExecuteGrantStmt (ACL/permission management)
  - objectNamesToOids (object name resolution)
  - find_all_inheritors (inheritance processing)
  - GetRelationPublications (publication management)
  - CreateExtensionInternal (extension management)
  - transformInsertStmt (INSERT statement processing)

## Notes and Other Information
- Specialized for OID values only, ensuring type safety in PostgreSQL's object reference system
- Extensively used for managing database object collections and relationships
- Must use return value as function may reallocate the list structure
- Critical for catalog operations, ACL management, and object dependency tracking
- One of the core functions for PostgreSQL's object identifier management system