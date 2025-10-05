# ATAddForeignKeyConstraint

## Location
[src/backend/commands/tablecmds.c:9607-10047](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L9607-L10047)

## Overview
ATAddForeignKeyConstraint implements the complex logic for adding foreign key constraints to tables, including comprehensive validation, operator resolution, and handling of partitioned table hierarchies.

## Definition

```c
static ObjectAddress
ATAddForeignKeyConstraint(List **wqueue, AlteredTableInfo *tab, Relation rel,
						  Constraint *fkconstraint,
						  bool recurse, bool recursing, LOCKMODE lockmode)
```
## Detailed Description
This function is one of PostgreSQL's most complex constraint implementation functions, handling the complete lifecycle of foreign key constraint creation. It performs extensive validation of referencing and referenced tables, resolves appropriate equality operators for each column pair, handles table persistence compatibility checks, and manages the intricate requirements of partitioned table foreign keys.

The function implements sophisticated operator resolution logic to find appropriate equality operators for comparing foreign key columns with primary key columns, including support for implicit type coercion and polymorphic types. It validates that the referenced columns form a unique constraint and checks permissions on both sides of the relationship.

For partitioned tables, the function coordinates the creation of multiple pg_constraint entries and associated triggers across all partitions. It includes optimization logic to avoid revalidating existing data when constraints are modified in compatible ways, and handles the complex case of generated columns with appropriate action restrictions.

The implementation follows a three-phase approach: first creating the catalog entry, then processing action triggers on the referenced side, and finally creating check triggers on the referencing side, with appropriate recursion handling for inheritance hierarchies.

## Parameters / Member Variables
- `**wqueue`: Double pointer to the work queue for coordinating ALTER TABLE operations across multiple tables
- `*tab`: AlteredTableInfo structure containing information about the table being altered
- `rel`: Relation object representing the referencing (foreign key) table
- `*fkconstraint`: Constraint specification containing all foreign key definition details
- `recurse`: Boolean indicating whether to apply the constraint to inheritance children
- `recursing`: Boolean indicating if this is a recursive call (affects permission handling)
- `lockmode`: Lock mode to use when accessing related tables during the operation
## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md), table_openrv
  - [transformColumnNameList](../t/transformColumnNameList.md)
  - [transformFkeyGetPrimaryKey](../t/transformFkeyGetPrimaryKey.md)
  - [transformFkeyCheckAttrs](../t/transformFkeyCheckAttrs.md)
  - [checkFkeyPermissions](../c/checkFkeyPermissions.md)
  - [validateFkOnDeleteSetColumns](../v/validateFkOnDeleteSetColumns.md)
  - [get_opfamily_member](../g/get_opfamily_member.md)
  - [can_coerce_type](../c/can_coerce_type.md)
  - [findFkeyCast](../f/findFkeyCast.md)
  - [addFkConstraint](../a/addFkConstraint.md)
  - [addFkRecurseReferenced](../a/addFkRecurseReferenced.md)
  - [addFkRecurseReferencing](../a/addFkRecurseReferencing.md)
- Called from (representative examples):
  - [ATExecAddConstraint](ATExecAddConstraint.md)

## Notes and Other Information
- Handles complex operator resolution for type compatibility between foreign and primary key columns
- Implements comprehensive validation including table persistence compatibility (permanent/unlogged/temporary)
- Supports optimization to avoid revalidation when constraints are modified in compatible ways
- Manages the intricate requirements of partitioned table foreign keys with multiple constraint entries
- Enforces restrictions on generated columns according to SQL standard requirements
- Coordinates trigger creation on both referencing and referenced sides of the relationship
- Includes sophisticated error handling with detailed diagnostic messages for incompatible types
- Integrates with PostgreSQL's work queue system for managing complex multi-table operations
- Essential component of PostgreSQL's referential integrity implementation
- One of the most complex functions in the ALTER TABLE subsystem due to the inherent complexity of foreign key semantics

## Simplified Source

```c
static ObjectAddress ATAddForeignKeyConstraint(List **wqueue, AlteredTableInfo *tab, Relation rel,
                                              Constraint *fkconstraint,
                                              bool recurse, bool recursing, LOCKMODE lockmode) {
    Relation pkrel;
    int16 pkattnum[INDEX_MAX_KEYS] = {0};
    int16 fkattnum[INDEX_MAX_KEYS] = {0};
    Oid pktypoid[INDEX_MAX_KEYS] = {0};
    Oid fktypoid[INDEX_MAX_KEYS] = {0};
    Oid opclasses[INDEX_MAX_KEYS] = {0};
    Oid pfeqoperators[INDEX_MAX_KEYS] = {0};
    Oid ppeqoperators[INDEX_MAX_KEYS] = {0};
    Oid ffeqoperators[INDEX_MAX_KEYS] = {0};
    int16 fkdelsetcols[INDEX_MAX_KEYS] = {0};
    int numfks, numpks, numfkdelsetcols;
    Oid indexOid;
    bool old_check_ok;
    ObjectAddress address;

    // Open referenced table with appropriate lock
    if (OidIsValid(fkconstraint->old_pktable_oid))
        pkrel = table_open(fkconstraint->old_pktable_oid, ShareRowExclusiveLock);
    else
        pkrel = table_openrv(fkconstraint->pktable, ShareRowExclusiveLock);

    // Validate table types and persistence compatibility
    if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE) {
        if (!recurse)
            ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                           errmsg("cannot use ONLY for foreign key on partitioned table")));
    }

    // Check table persistence compatibility (temp/permanent/unlogged)
    switch (rel->rd_rel->relpersistence) {
        case RELPERSISTENCE_PERMANENT:
            if (!RelationIsPermanent(pkrel))
                ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                               errmsg("constraints on permanent tables may reference only permanent tables")));
            break;
        case RELPERSISTENCE_TEMP:
            if (pkrel->rd_rel->relpersistence != RELPERSISTENCE_TEMP)
                ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                               errmsg("constraints on temporary tables may reference only temporary tables")));
            break;
        // ... other persistence checks
    }

    // Transform and validate column lists
    numfks = transformColumnNameList(RelationGetRelid(rel), fkconstraint->fk_attrs,
                                    fkattnum, fktypoid);

    if (fkconstraint->pk_attrs == NIL) {
        // Use primary key if no columns specified
        numpks = transformFkeyGetPrimaryKey(pkrel, &indexOid, &fkconstraint->pk_attrs,
                                           pkattnum, pktypoid, opclasses);
    } else {
        // Validate specified columns
        numpks = transformColumnNameList(RelationGetRelid(pkrel), fkconstraint->pk_attrs,
                                        pkattnum, pktypoid);
        indexOid = transformFkeyCheckAttrs(pkrel, numpks, pkattnum, opclasses);
    }

    // Check permissions and validate column count
    checkFkeyPermissions(pkrel, pkattnum, numpks);
    if (numfks != numpks)
        ereport(ERROR, (errcode(ERRCODE_INVALID_FOREIGN_KEY),
                       errmsg("number of referencing and referenced columns disagree")));

    // Resolve equality operators for each column pair
    old_check_ok = (fkconstraint->old_conpfeqop != NIL);
    for (int i = 0; i < numpks; i++) {
        Oid pktype = pktypoid[i];
        Oid fktype = fktypoid[i];

        // Find appropriate equality operators (simplified logic)
        Oid pfeqop = get_opfamily_member(opfamily, opcintype, fktype, BTEqualStrategyNumber);
        Oid ppeqop = get_opfamily_member(opfamily, opcintype, opcintype, BTEqualStrategyNumber);
        Oid ffeqop = get_opfamily_member(opfamily, fktype, fktype, BTEqualStrategyNumber);

        if (!OidIsValid(pfeqop) || !OidIsValid(ffeqop))
            ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                           errmsg("foreign key constraint cannot be implemented"),
                           errdetail("Key columns are of incompatible types")));

        pfeqoperators[i] = pfeqop;
        ppeqoperators[i] = ppeqop;
        ffeqoperators[i] = ffeqop;
    }

    // Create the constraint catalog entry
    address = addFkConstraint(addFkBothSides, fkconstraint->conname, fkconstraint,
                             rel, pkrel, indexOid, InvalidOid, numfks,
                             pkattnum, fkattnum, pfeqoperators, ppeqoperators, ffeqoperators,
                             numfkdelsetcols, fkdelsetcols, false);

    // Add action triggers on referenced side and recurse
    addFkRecurseReferenced(fkconstraint, rel, pkrel, indexOid, address.objectId,
                          numfks, pkattnum, fkattnum, pfeqoperators, ppeqoperators, ffeqoperators,
                          numfkdelsetcols, fkdelsetcols, old_check_ok, InvalidOid, InvalidOid);

    // Add check triggers on referencing side and recurse
    addFkRecurseReferencing(wqueue, fkconstraint, rel, pkrel, indexOid, address.objectId,
                           numfks, pkattnum, fkattnum, pfeqoperators, ppeqoperators, ffeqoperators,
                           numfkdelsetcols, fkdelsetcols, old_check_ok, lockmode, InvalidOid, InvalidOid);

    table_close(pkrel, NoLock);
    return address;
}
```