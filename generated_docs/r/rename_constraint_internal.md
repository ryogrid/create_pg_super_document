# rename_constraint_internal

## Location
[src/backend/commands/tablecmds.c:3915-4020](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L3915-L4020)

## Overview
rename_constraint_internal is the core internal function that performs constraint renaming operations for both table and domain constraints, handling inheritance hierarchies and constraint type-specific logic.

## Definition
static ObjectAddress rename_constraint_internal(Oid myrelid, Oid mytypid, const char *oldconname, const char *newconname, bool recurse, bool recursing, int expected_parents)

## Detailed Description
rename_constraint_internal implements the logic for renaming constraints on both relations and domains. The function handles different constraint types appropriately - for indexed constraints (PRIMARY KEY, UNIQUE, EXCLUSION), it renames the underlying index which automatically renames the constraint. For other constraint types, it directly calls RenameConstraintById.

The function includes comprehensive inheritance handling for CHECK constraints, recursively renaming constraints in child tables when requested, or enforcing that inheritance hierarchies are handled correctly when recursion is disabled. It performs validation through renameatt_check for relation constraints and manages cache invalidation to ensure consistency.

## Parameters / Member Variables
- `myrelid`: OID of the relation containing the constraint (0 if domain constraint)
- `mytypid`: OID of the domain type (0 if relation constraint)  
- `oldconname`: Current name of the constraint to rename
- `newconname`: New name for the constraint
- `recurse`: Whether to recursively rename constraints in child tables
- `recursing`: Whether this call is part of a recursive operation
- `expected_parents`: Expected number of parent tables inheriting this constraint

## Dependencies
- Functions called/Symbols referenced:
  - [get_domain_constraint_oid](../g/get_domain_constraint_oid.md)
  - [get_relation_constraint_oid](../g/get_relation_constraint_oid.md)
  - [relation_open](relation_open.md)
  - [renameatt_check](renameatt_check.md)
  - [find_all_inheritors](../f/find_all_inheritors.md)
  - [find_inheritance_children](../f/find_inheritance_children.md)
  - [RenameRelationInternal](../R/RenameRelationInternal.md)
  - [RenameConstraintById](../R/RenameConstraintById.md)
  - [CacheInvalidateRelcache](../C/CacheInvalidateRelcache.md)
  - [relation_close](relation_close.md)
  - ObjectAddressSet
- Called from (representative examples):
  - [RenameConstraint](../R/RenameConstraint.md) (in src/backend/commands/tablecmds.c)
  - [rename_constraint_internal](rename_constraint_internal.md) (recursive calls)

## Notes and Other Information
- Uses AccessExclusiveLock when opening relations to prevent concurrent modifications
- Handles both relation constraints and domain constraints through separate code paths
- For indexed constraints, renames the underlying index rather than the constraint directly
- Validates inheritance relationships and prevents incorrect constraint renaming in inheritance hierarchies
- Performs cache invalidation to ensure other sessions see the constraint name change
- Similar logic structure to renameatt_internal for consistency
- Supports both recursive and non-recursive constraint renaming operations

## Simplified Source

```c
static ObjectAddress rename_constraint_internal(Oid myrelid, Oid mytypid, const char *oldconname,
                                               const char *newconname, bool recurse, bool recursing,
                                               int expected_parents) {
    Relation targetrelation = NULL;
    Oid constraintOid;
    HeapTuple tuple;
    Form_pg_constraint con;
    ObjectAddress address;

    Assert(!myrelid || !mytypid); // Either relation or domain, not both

    // Get constraint OID
    if (mytypid) {
        // Domain constraint
        constraintOid = get_domain_constraint_oid(mytypid, oldconname, false);
    } else {
        // Relation constraint
        targetrelation = relation_open(myrelid, AccessExclusiveLock);
        renameatt_check(myrelid, RelationGetForm(targetrelation), false);
        constraintOid = get_relation_constraint_oid(myrelid, oldconname, false);
    }

    // Get constraint information
    tuple = SearchSysCache1(CONSTROID, ObjectIdGetDatum(constraintOid));
    if (!HeapTupleIsValid(tuple))
        elog(ERROR, "cache lookup failed for constraint %u", constraintOid);
    con = (Form_pg_constraint) GETSTRUCT(tuple);

    // Handle inheritance for CHECK constraints
    if (myrelid && con->contype == CONSTRAINT_CHECK && !con->connoinherit) {
        if (recurse) {
            // Recursively rename in child tables
            List *child_oids, *child_numparents;
            ListCell *lo, *li;

            child_oids = find_all_inheritors(myrelid, AccessExclusiveLock, &child_numparents);
            forboth(lo, child_oids, li, child_numparents) {
                Oid childrelid = lfirst_oid(lo);
                int numparents = lfirst_int(li);

                if (childrelid == myrelid)
                    continue;

                rename_constraint_internal(childrelid, InvalidOid, oldconname, newconname,
                                         false, true, numparents);
            }
        } else {
            // Check inheritance rules if not recursing
            if (expected_parents == 0 && find_inheritance_children(myrelid, NoLock) != NIL)
                ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                               errmsg("inherited constraint \"%s\" must be renamed in child tables too",
                                      oldconname)));
        }

        // Check inheritance count
        if (con->coninhcount > expected_parents)
            ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                           errmsg("cannot rename inherited constraint \"%s\"", oldconname)));
    }

    // Perform the actual rename
    if (con->conindid && (con->contype == CONSTRAINT_PRIMARY ||
                         con->contype == CONSTRAINT_UNIQUE ||
                         con->contype == CONSTRAINT_EXCLUSION)) {
        // For indexed constraints, rename the index (which renames the constraint)
        RenameRelationInternal(con->conindid, newconname, false, true);
    } else {
        // For other constraints, rename directly
        RenameConstraintById(constraintOid, newconname);
    }

    ObjectAddressSet(address, ConstraintRelationId, constraintOid);
    ReleaseSysCache(tuple);

    // Clean up relation if opened
    if (targetrelation) {
        CacheInvalidateRelcache(targetrelation);
        relation_close(targetrelation, NoLock);
    }

    return address;
}
```