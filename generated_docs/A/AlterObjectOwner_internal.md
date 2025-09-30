# AlterObjectOwner_internal

## Location
[src/backend/commands/alter.c:917-1054](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/alter.c#L917-L1054)

## Overview
A generic internal function that changes the ownership of a database object by updating its catalog entry, handling permissions, ACL modifications, and dependency updates.

## Definition

```c
void
AlterObjectOwner_internal(Oid classId, Oid objectId, Oid new_ownerId)
```
## Detailed Description
AlterObjectOwner_internal provides the core implementation for changing object ownership in PostgreSQL. This function handles the common case where ownership alteration only requires updating a single catalog entry's owner column along with associated ACL and dependency changes. It performs comprehensive validation and updates multiple aspects of object ownership.

The function operates through several key phases:
1. **Catalog identification**: Determines the correct catalog to modify (special handling for large objects)
2. **Object retrieval**: Fetches and locks the object tuple using extended catalog lookup
3. **Early exit optimization**: Returns immediately if the owner is already correct
4. **Permission validation**: Ensures current user can change ownership and new owner can accept it
5. **ACL updates**: Modifies access control lists when present to reflect the new owner
6. **Catalog update**: Updates the owner column and potentially ACL column in the catalog tuple
7. **Dependency update**: Updates the dependency system to reflect the new ownership relationship

Special handling is included for large objects, where the catalog to modify is pg_largeobject_metadata rather than the class ID itself. The function also validates namespace privileges when applicable.

## Parameters / Member Variables
- : OID of the object's catalog class (e.g., ProcedureRelationId, TypeRelationId)
- : OID of the specific object whose ownership should be changed
- : OID of the role that will become the new owner

## Dependencies
- Functions called/Symbols referenced:
  - : Gets attribute numbers for OID, owner, namespace, ACL, and name columns
  - : Opens catalog relation with RowExclusiveLock
  - : Extended catalog object lookup with locking
  - : Extracts attribute values from catalog tuples
  - : Checks if current user is superuser (bypasses permission checks)
  - : Validates current user has privileges of the current owner
  - : Validates current user can become the new owner
  - : Checks CREATE privilege on namespace for new owner
  - : Creates new ACL with updated owner
  - : Creates modified catalog tuple
  - : Performs the actual catalog update
  - : Releases tuple locks
  - : Updates ownership dependency records
  - : Fires post-alteration hooks
- Called from (representative examples):
  - : Main ALTER OWNER statement execution
  - : Shared dependency ownership reassignment

## Notes and Other Information
- Returns void - operates through side effects on catalog and dependency system
- Designed for simple cases - won't work for complex objects like tables requiring additional processing
- Special handling for large objects where catalogId differs from classId
- Includes optimization to avoid unnecessary work when ownership is already correct
- Performs comprehensive permission checking unless the user is a superuser
- Updates both the owner column and ACL when ACL is present
- Uses tuple locking to ensure safe concurrent access during the ownership change
- Validates that new owner has CREATE privileges on the object's namespace if applicable
- Memory management includes proper cleanup of allocated arrays for tuple modification
- Uses extended catalog lookup to ensure proper tuple locking behavior

## Simplified Source

```c
void AlterObjectOwner_internal(Oid classId, Oid objectId, Oid new_ownerId)
{
    // Handle special case for large objects
    Oid catalogId = (classId == LargeObjectRelationId) ?
                    LargeObjectMetadataRelationId : classId;

    // Get attribute numbers for key columns
    AttrNumber owner_col = get_object_attnum_owner(catalogId);
    AttrNumber namespace_col = get_object_attnum_namespace(catalogId);
    AttrNumber acl_col = get_object_attnum_acl(catalogId);

    // Open catalog and find object
    Relation rel = table_open(catalogId, RowExclusiveLock);
    HeapTuple oldtup = get_catalog_object_by_oid_extended(rel, objectId, true);

    if (!oldtup)
        elog(ERROR, "object %u not found", objectId);

    // Get current owner and namespace
    Oid old_owner = get_tuple_owner(oldtup, rel, owner_col);
    Oid namespace_oid = get_tuple_namespace(oldtup, rel, namespace_col);

    // Early exit if owner already correct
    if (old_owner == new_ownerId) {
        unlock_and_close(rel, oldtup);
        return;
    }

    // Permission checks (unless superuser)
    if (!superuser()) {
        check_ownership_permissions(old_owner, new_ownerId, namespace_oid);
    }

    // Build modified tuple with new owner
    HeapTuple newtup = build_owner_update_tuple(oldtup, rel,
                                                new_ownerId, old_owner,
                                                owner_col, acl_col);

    // Update catalog
    CatalogTupleUpdate(rel, &newtup->t_self, newtup);

    // Update dependency system
    changeDependencyOnOwner(classId, objectId, new_ownerId);

    // Cleanup and fire hooks
    unlock_and_close(rel, oldtup);
    InvokeObjectPostAlterHook(classId, objectId, 0);
}
```