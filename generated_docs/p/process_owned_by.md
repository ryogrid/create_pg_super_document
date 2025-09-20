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
- : The sequence relation being modified
- : List containing either "none" or table.column specification
- : Boolean flag indicating if this is for an identity sequence (determines dependency type)

## Dependencies
- Functions called/Symbols referenced:
  - list_length
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