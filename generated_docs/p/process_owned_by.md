# process_owned_by

## Location
[src/backend/commands/sequence.c:1593-1706](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/sequence.c#L1593-L1706)

## Overview
Processes an OWNED BY option for CREATE/ALTER SEQUENCE statements, establishing or removing dependency relationships between sequences and table columns while enforcing ownership and schema constraints.

## Definition

```c
static void
process_owned_by(Relation seqrel, List *owned_by, bool for_identity)
```
## Detailed Description
The  function handles the OWNED BY clause in sequence operations, which establishes a dependency between a sequence and a table column. This dependency ensures that when the owning table column is dropped, the sequence is automatically dropped as well. The function performs several critical validations:

1. **Ownership validation**: Ensures the sequence and referenced table have the same owner
2. **Schema validation**: Ensures the sequence and referenced table are in the same schema  
3. **Relation type validation**: Verifies the referenced relation is a regular table, foreign table, view, or partitioned table
4. **Identity sequence protection**: Prevents manual modification of sequences owned by identity columns
5. **Dependency management**: Updates pg_depend catalog to reflect the new ownership relationship

The function supports two dependency types: DEPENDENCY_AUTO for regular sequences and DEPENDENCY_INTERNAL for identity sequences.

## Parameters / Member Variables
- `seqrel`: The sequence relation being modified
- `*owned_by`: List containing either "none" or table.column specification
- `for_identity`: Boolean flag indicating if this is for an identity sequence (determines dependency type)
## Dependencies
- Functions called/Symbols referenced:
  - [list_length](../l/list_length.md)
  - strVal
  - linitial
  - [list_copy_head](../l/list_copy_head.md)
  - llast
  - [makeRangeVarFromNameList](../m/makeRangeVarFromNameList.md)
  - [relation_openrv](../r/relation_openrv.md)
  - RelationGetRelationName
  - RelationGetNamespace
  - [get_attnum](../g/get_attnum.md)
  - RelationGetRelid
  - [sequenceIsOwned](../s/sequenceIsOwned.md)
  - [get_rel_name](../g/get_rel_name.md)
  - [deleteDependencyRecordsForClass](../d/deleteDependencyRecordsForClass.md)
  - [recordDependencyOn](../r/recordDependencyOn.md)
  - [relation_close](../r/relation_close.md)
- Called from:
  - [DefineSequence](../D/DefineSequence.md)
  - [AlterSequence](../A/AlterSequence.md)

## Notes and Other Information
- This function is critical for maintaining referential integrity between sequences and their owning tables
- The "OWNED BY NONE" option removes any existing ownership dependency
- Identity sequences have special protection against manual ownership changes
- The function holds locks on referenced tables until transaction commit to prevent concurrent modifications
- Error handling includes specific error codes for different validation failures (ERRCODE_SYNTAX_ERROR, ERRCODE_WRONG_OBJECT_TYPE, etc.)
- Located in src/backend/commands/sequence.c:1593-1706

## Simplified Source

```c
static void
process_owned_by(Relation seqrel, List *owned_by, bool for_identity)
{
    DependencyType deptype;
    int nnames;
    Relation tablerel;
    AttrNumber attnum;

    // Set dependency type: INTERNAL for identity, AUTO for regular sequences
    deptype = for_identity ? DEPENDENCY_INTERNAL : DEPENDENCY_AUTO;

    nnames = list_length(owned_by);

    if (nnames == 1) {
        // Handle "OWNED BY NONE" case
        if (strcmp(strVal(linitial(owned_by)), "none") != 0)
            ereport(ERROR, "invalid OWNED BY option");
        tablerel = NULL;
        attnum = 0;
    }
    else {
        // Parse table.column specification
        List *relname = list_copy_head(owned_by, nnames - 1);
        char *attrname = strVal(llast(owned_by));

        // Open and lock the referenced table
        RangeVar *rel = makeRangeVarFromNameList(relname);
        tablerel = relation_openrv(rel, AccessShareLock);

        // Validate relation type (table, foreign table, view, partitioned table)
        if (!(tablerel->rd_rel->relkind == RELKIND_RELATION ||
              tablerel->rd_rel->relkind == RELKIND_FOREIGN_TABLE ||
              tablerel->rd_rel->relkind == RELKIND_VIEW ||
              tablerel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE))
            ereport(ERROR, "sequence cannot be owned by this relation type");

        // Enforce same owner constraint
        if (seqrel->rd_rel->relowner != tablerel->rd_rel->relowner)
            ereport(ERROR, "sequence must have same owner as table");

        // Enforce same schema constraint
        if (RelationGetNamespace(seqrel) != RelationGetNamespace(tablerel))
            ereport(ERROR, "sequence must be in same schema as table");

        // Get attribute number and validate column exists
        attnum = get_attnum(RelationGetRelid(tablerel), attrname);
        if (attnum == InvalidAttrNumber)
            ereport(ERROR, "column does not exist");
    }

    // Prevent manual changes to identity sequence ownership
    if (deptype == DEPENDENCY_AUTO) {
        Oid tableId;
        int32 colId;
        if (sequenceIsOwned(RelationGetRelid(seqrel), DEPENDENCY_INTERNAL, &tableId, &colId))
            ereport(ERROR, "cannot change ownership of identity sequence");
    }

    // Update pg_depend: remove existing dependencies
    deleteDependencyRecordsForClass(RelationRelationId, RelationGetRelid(seqrel),
                                    RelationRelationId, deptype);

    // Add new dependency if not "OWNED BY NONE"
    if (tablerel) {
        ObjectAddress refobject, depobject;

        refobject.classId = RelationRelationId;
        refobject.objectId = RelationGetRelid(tablerel);
        refobject.objectSubId = attnum;
        depobject.classId = RelationRelationId;
        depobject.objectId = RelationGetRelid(seqrel);
        depobject.objectSubId = 0;

        recordDependencyOn(&depobject, &refobject, deptype);
    }

    // Close table but hold lock until commit
    if (tablerel)
        relation_close(tablerel, NoLock);
}
```