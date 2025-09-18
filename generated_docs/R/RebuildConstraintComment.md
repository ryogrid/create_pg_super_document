# RebuildConstraintComment

## Location
src/backend/commands/tablecmds.c: 14247 - 14290

## Overview
RebuildConstraintComment recreates comments for table or domain constraints that are being rebuilt during ALTER TABLE operations.

## Definition
```c
static void RebuildConstraintComment(AlteredTableInfo *tab, AlterTablePass pass, Oid objid, Relation rel, List *domname, const char *conname)
```

## Detailed Description
This function is a specialized subroutine used by ATPostAlterTypeParse to preserve constraint comments during constraint rebuilding operations. When constraints are dropped and recreated due to column type changes, their associated comments would normally be lost. This function retrieves the original comment from the pg_constraint catalog and creates a new CommentStmt command that will be executed to restore the comment after the constraint is recreated. It handles both table constraints and domain constraints, constructing the appropriate object identifier for each type.

## Parameters / Member Variables
- `tab`: Pointer to AlteredTableInfo structure containing the work queues
- `pass`: The ALTER TABLE pass where the comment restoration command should be queued
- `objid`: OID of the original constraint whose comment needs to be preserved
- `rel`: Relation pointer for table constraints (NULL for domain constraints)
- `domname`: List containing the qualified domain name for domain constraints (NULL for table constraints)
- `conname`: Name of the constraint

## Dependencies
- Functions called/Symbols referenced:
  - [GetComment](../G/GetComment.md)
  - makeNode
  - [get_namespace_name](../g/get_namespace_name.md)
  - RelationGetNamespace
  - RelationGetRelationName
  - [makeString](../m/makeString.md)
  - [pstrdup](../p/pstrdup.md)
  - list_make3
  - list_make2
  - makeTypeNameFromNameList
  - copyObject
  - lappend
  - CommentStmt (struct)
  - AlterTableCmd (struct)
  - [AlteredTableInfo](../A/AlteredTableInfo.md) (struct)
  - [AlterTablePass](../A/AlterTablePass.md) (enum)
- Called from (representative examples):
  - [ATPostAlterTypeParse](../A/ATPostAlterTypeParse.md) (multiple calls for different constraint types)
  - child_dependency_type

## Notes and Other Information
- Only creates a comment restoration command if the original constraint actually had a comment
- Distinguishes between table constraints (OBJECT_TABCONSTRAINT) and domain constraints (OBJECT_DOMCONSTRAINT)
- Constructs fully qualified object names to avoid ambiguity during comment restoration
- Uses AT_ReAddComment subtype for the generated command
- Essential for maintaining constraint documentation during ALTER TABLE operations that require constraint rebuilding