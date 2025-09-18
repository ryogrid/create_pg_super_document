# SetSecurityLabel

## Location
[src/backend/commands/seclabel.c:404-490](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/seclabel.c#L404-L490)

## Overview
SetSecurityLabel sets or deletes a security label for a specified database object with a given security provider.

## Definition


## Detailed Description
SetSecurityLabel attempts to set the security label for the specified provider on the specified object to the given value. If the label parameter is NULL, any existing label is deleted. The function handles both regular objects (stored in pg_seclabel) and shared objects (which have their own security label catalog and are handled via SetSharedSecurityLabel).

The function performs the following operations:
1. Checks if the object is a shared relation and delegates to SetSharedSecurityLabel if so
2. Searches for an existing security label entry using a system catalog scan
3. If an existing entry is found:
   - Deletes it if the new label is NULL
   - Updates it with the new label value if the label is not NULL
4. If no existing entry is found and a label is provided, inserts a new tuple
5. Properly maintains catalog indexes and cleans up memory

## Parameters / Member Variables
- : Pointer to ObjectAddress structure identifying the target database object (contains classId, objectId, and objectSubId)
- : String identifying the security label provider (e.g., 'selinux')
- : The security label string to set, or NULL to delete any existing label

## Dependencies
- Functions called/Symbols referenced:
  - [IsSharedRelation](../I/IsSharedRelation.md)
  - [SetSharedSecurityLabel](SetSharedSecurityLabel.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md)
  - [heap_modify_tuple](../h/heap_modify_tuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [CatalogTupleInsert](../C/CatalogTupleInsert.md)
  - [heap_freetuple](../h/heap_freetuple.md)
- Called from (representative examples):
  - [ExecSecLabelStmt](../E/ExecSecLabelStmt.md)

## Notes and Other Information
- The function distinguishes between shared and non-shared objects, routing shared objects to SetSharedSecurityLabel
- Uses the SecLabelObjectIndexId index for efficient searching of existing labels
- Properly handles memory management by freeing heap tuples when done
- Maintains transactional consistency by using RowExclusiveLock on the pg_seclabel relation
- The function is the primary entry point for the SECURITY LABEL SQL command execution