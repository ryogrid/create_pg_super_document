# ATExecAddIndexConstraint

## Location
[src/backend/commands/tablecmds.c:9263-9354](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L9263-L9354)

## Overview
ATExecAddIndexConstraint implements the ALTER TABLE ADD CONSTRAINT USING INDEX command, which creates a primary key or unique constraint using an existing unique index.

## Definition

```c
static ObjectAddress
ATExecAddIndexConstraint(AlteredTableInfo *tab, Relation rel,
						 IndexStmt *stmt, LOCKMODE lockmode)
```
## Detailed Description
This function executes the ALTER TABLE ADD CONSTRAINT USING INDEX operation by taking an existing unique index and converting it into a table constraint (either PRIMARY KEY or UNIQUE). The function validates that the specified index is unique, handles constraint naming (renaming the index if necessary to match the constraint name), and creates the appropriate catalog entries. It ensures the constraint and index have the same name as required by PostgreSQL's design.

The function performs several key validations: it rejects operations on partitioned tables (not currently supported), verifies the index is unique, and performs additional checks for primary key constraints. When creating primary key constraints, it calls index_check_primary_key to ensure all necessary conditions are met.

## Parameters / Member Variables
- : AlteredTableInfo structure containing information about the table being altered
- : Relation object representing the table to which the constraint is being added
- : IndexStmt containing the constraint specification, including the index OID and constraint properties
- : Lock mode to use during the operation (though not directly used in this function)

## Dependencies
- Functions called/Symbols referenced:
  - [index_open](../i/index_open.md)
  - [BuildIndexInfo](../B/BuildIndexInfo.md)
  - [RenameRelationInternal](../R/RenameRelationInternal.md)
  - [index_check_primary_key](../i/index_check_primary_key.md)
  - [index_constraint_create](../i/index_constraint_create.md)
  - [index_close](../i/index_close.md)
- Called from (representative examples):
  - [ATExecCmd](ATExecCmd.md) (main ALTER TABLE command dispatcher)

## Notes and Other Information
- Currently does not support partitioned tables and will raise an error if attempted
- Requires the underlying index to be unique, which should be validated at parse time
- Enforces naming consistency between constraints and indexes by renaming the index if necessary
- Supports both PRIMARY KEY and UNIQUE constraints, but not EXCLUSION constraints
- Handles deferred and deferrable constraint options through appropriate flags
- Returns an ObjectAddress for the newly created constraint for dependency tracking

## Simplified Source

```c
static ObjectAddress
ATExecAddIndexConstraint(AlteredTableInfo *tab, Relation rel,
                        IndexStmt *stmt, LOCKMODE lockmode)
{
    Oid index_oid = stmt->indexOid;
    Relation indexRel;
    char *indexName;
    char *constraintName;
    char constraintType;
    ObjectAddress address;
    bits16 flags;

    // Reject partitioned tables - not supported
    if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
        ereport(ERROR, "ADD CONSTRAINT USING INDEX not supported on partitioned tables");

    // Open the index and get its info
    indexRel = index_open(index_oid, AccessShareLock);
    indexName = pstrdup(RelationGetRelationName(indexRel));
    IndexInfo *indexInfo = BuildIndexInfo(indexRel);

    // Verify index is unique
    if (!indexInfo->ii_Unique)
        elog(ERROR, "index \"%s\" is not unique", indexName);

    // Handle constraint naming - constraint and index must have same name
    constraintName = stmt->idxname;
    if (constraintName == NULL)
        constraintName = indexName;
    else if (strcmp(constraintName, indexName) != 0) {
        // Rename index to match constraint name
        ereport(NOTICE, "will rename index \"%s\" to \"%s\"", indexName, constraintName);
        RenameRelationInternal(index_oid, constraintName, false, true);
    }

    // Additional validation for primary key constraints
    if (stmt->primary)
        index_check_primary_key(rel, indexInfo, true, stmt);

    // Determine constraint type
    constraintType = stmt->primary ? CONSTRAINT_PRIMARY : CONSTRAINT_UNIQUE;

    // Set up flags for constraint creation
    flags = INDEX_CONSTR_CREATE_UPDATE_INDEX |
            INDEX_CONSTR_CREATE_REMOVE_OLD_DEPS |
            (stmt->initdeferred ? INDEX_CONSTR_CREATE_INIT_DEFERRED : 0) |
            (stmt->deferrable ? INDEX_CONSTR_CREATE_DEFERRABLE : 0) |
            (stmt->primary ? INDEX_CONSTR_CREATE_MARK_AS_PRIMARY : 0);

    // Create the constraint catalog entries
    address = index_constraint_create(rel, index_oid, InvalidOid, indexInfo,
                                    constraintName, constraintType, flags,
                                    allowSystemTableMods, false);

    index_close(indexRel, NoLock);
    return address;
}
```