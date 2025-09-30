# RangeVarCallbackForAlterRelation

## Location
[src/backend/commands/tablecmds.c:17847-17987](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L17847-L17987)

## Overview
A comprehensive callback function for RangeVarGetRelid that handles authorization and validation for relation rename, schema change, and alter table operations by ensuring ownership requirements and enforcing type-specific operation constraints.

## Definition
```c
static void RangeVarCallbackForAlterRelation(const RangeVar *rv, Oid relid, Oid oldrelid, void *arg)
```

## Detailed Description
This function serves as a sophisticated validation callback for multiple relation alteration operations including RENAME, SET SCHEMA, and ALTER TABLE commands. It performs comprehensive security checks, validates operation compatibility with relation types, and enforces PostgreSQL's object model constraints.

The function first verifies ownership and system catalog modification permissions similar to RangeVarCallbackOwnsRelation. It then extracts the specific operation type from the statement parse tree and validates that the requested operation is appropriate for the target relation's type. For RENAME operations, it additionally checks CREATE privileges on the containing namespace.

The function implements strict type checking to prevent inappropriate operations (e.g., using ALTER SEQUENCE on a table) while maintaining backward compatibility by allowing ALTER TABLE on most relation types except composite types.

## Parameters / Member Variables
- `rv`: Pointer to RangeVar structure containing relation name and schema information
- `relid`: Object identifier of the target relation
- `oldrelid`: Previous relation ID for concurrent operations (unused in this implementation)
- `arg`: Generic argument pointer containing the parsed statement (RenameStmt, AlterObjectSchemaStmt, or AlterTableStmt)

## Dependencies
- Functions called/Symbols referenced:
  - [object_ownercheck](../o/object_ownercheck.md) - Verifies user ownership of database objects
  - [aclcheck_error](../a/aclcheck_error.md) - Reports access control violations
  - [get_relkind_objtype](../g/get_relkind_objtype.md), get_rel_relkind - [Relation](Relation.md) type utilities
  - [IsSystemClass](../I/IsSystemClass.md) - System catalog validation
  - [object_aclcheck](../o/object_aclcheck.md) - Namespace permission checking
  - [get_namespace_name](../g/get_namespace_name.md) - Namespace name resolution
  - nodeTag - Statement type identification
- Called from (representative examples):
  - [RenameRelation](RenameRelation.md) (src/backend/commands/tablecmds.c:4096)
  - [AlterTableLookupRelation](../A/AlterTableLookupRelation.md) (src/backend/commands/tablecmds.c:4344)
  - [AlterTableNamespace](../A/AlterTableNamespace.md) (src/backend/commands/tablecmds.c:17219)

## Notes and Other Information
- Static function scope limits visibility to tablecmds.c module
- Handles three distinct statement types: RenameStmt, AlterObjectSchemaStmt, and AlterTableStmt
- Enforces strict type checking while maintaining backward compatibility for ALTER TABLE
- For RENAME operations, validates CREATE permission on the containing namespace
- Prevents schema changes on indexes, composite types, and TOAST tables with helpful error messages
- Uses PostgreSQL's standard error reporting with appropriate error codes and hints
- Part of the table command infrastructure in src/backend/commands/tablecmds.c (lines 17847-17987)

## Simplified Source

```c
static void RangeVarCallbackForAlterRelation(const RangeVar *rv, Oid relid, Oid oldrelid, void *arg)
{
    Node *stmt = (Node *) arg;
    ObjectType reltype;
    HeapTuple tuple;
    Form_pg_class classform;
    char relkind;

    // Get relation info from system catalog
    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(tuple))
        return; // concurrently dropped
    classform = (Form_pg_class) GETSTRUCT(tuple);
    relkind = classform->relkind;

    // Verify ownership
    if (!object_ownercheck(RelationRelationId, relid, GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER, get_relkind_objtype(get_rel_relkind(relid)), rv->relname);

    // Prevent system catalog modifications unless allowed
    if (!allowSystemTableMods && IsSystemClass(relid, classform))
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                       errmsg("permission denied: \"%s\" is a system catalog", rv->relname)));

    // Extract operation type and check permissions
    if (IsA(stmt, RenameStmt))
    {
        // For rename, check CREATE permission on namespace
        aclresult = object_aclcheck(NamespaceRelationId, classform->relnamespace, GetUserId(), ACL_CREATE);
        if (aclresult != ACLCHECK_OK)
            aclcheck_error(aclresult, OBJECT_SCHEMA, get_namespace_name(classform->relnamespace));
        reltype = ((RenameStmt *) stmt)->renameType;
    }
    else if (IsA(stmt, AlterObjectSchemaStmt))
        reltype = ((AlterObjectSchemaStmt *) stmt)->objectType;
    else if (IsA(stmt, AlterTableStmt))
        reltype = ((AlterTableStmt *) stmt)->objtype;
    else
        elog(ERROR, "unrecognized node type: %d", (int) nodeTag(stmt));

    // Validate operation compatibility with relation type
    if (reltype == OBJECT_SEQUENCE && relkind != RELKIND_SEQUENCE)
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                       errmsg("\"%s\" is not a sequence", rv->relname)));

    // Additional type validations for view, materialized view, foreign table, etc.
    // Block inappropriate operations like ALTER TABLE on composite types
    // Prevent schema changes on indexes, composite types, TOAST tables

    ReleaseSysCache(tuple);
}
```