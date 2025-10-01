# ATPrepAlterColumnType

## Location
[src/backend/commands/tablecmds.c:12807-13098](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L12807-L13098)

## Overview
Prepares ALTER COLUMN TYPE operations during Phase 1 of ALTER TABLE processing, handling type validation, expression transformation, and inheritance recursion.

## Definition

```c
static void
ATPrepAlterColumnType(List **wqueue,
					  AlteredTableInfo *tab, Relation rel,
					  bool recurse, bool recursing,
					  AlterTableCmd *cmd, LOCKMODE lockmode,
					  AlterTableUtilityContext *context)
```
## Detailed Description
This function performs Phase 1 preparation for ALTER COLUMN TYPE operations. Unlike other ALTER TABLE subcommands, it performs parse transformation during Phase 1 to ensure all USING expressions are parsed against the original table schema. The function validates the target column exists and is alterable, checks type compatibility and permissions, transforms USING expressions or creates default coercion expressions, determines if a table rewrite is required, and handles inheritance recursion with proper attribute number remapping. It supports both regular tables and typed tables, with special handling for generated columns, partition keys, and inherited columns.

## Parameters / Member Variables
- : Work queue for queueing additional ALTER TABLE commands
- : Information about the table being altered
- : The relation being altered
- : Whether to recursively process child tables
- : True when called recursively on child tables
- : The ALTER TABLE command containing column and type information
- : Lock level to use when accessing child relations
- : Utility context for additional ALTER TABLE processing

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCacheAttName](../S/SearchSysCacheAttName.md) (column lookup)
  - [has_partition_attrs](../h/has_partition_attrs.md) (partition key validation)
  - [typenameTypeIdAndMod](../t/typenameTypeIdAndMod.md) (type resolution)
  - [object_aclcheck](../o/object_aclcheck.md)/aclcheck_error_type (permission checking)
  - [GetColumnDefCollation](../G/GetColumnDefCollation.md) (collation handling)
  - [CheckAttributeType](../C/CheckAttributeType.md) (type validation)
  - [coerce_to_target_type](../c/coerce_to_target_type.md) (type coercion)
  - [assign_expr_collations](../a/assign_expr_collations.md) (expression processing)
  - [expression_planner](../e/expression_planner.md) (expression optimization)
  - [ATColumnChangeRequiresRewrite](ATColumnChangeRequiresRewrite.md) (rewrite determination)
  - [find_all_inheritors](../f/find_all_inheritors.md) (inheritance processing)
  - [build_attrmap_by_name](../b/build_attrmap_by_name.md)/map_variable_attnos (attribute mapping)
  - [ATTypedTableRecursion](ATTypedTableRecursion.md) (typed table handling)
- Called from (representative examples):
  - [ATPrepCmd](ATPrepCmd.md) (main ALTER TABLE preparation)

## Notes and Other Information
- Performs parse transformation during Phase 1 to handle USING expressions correctly
- USING expressions are parsed against the original table schema before any alterations
- Cannot alter system columns, inherited columns (at top level), or generated columns with USING
- Prevents altering columns used in partition keys
- For tables requiring rewrite, creates NewColumnValue entries for ATRewriteTable
- Uses custom recursion mechanism instead of ATSimpleRecursion for attribute remapping
- Handles both explicit USING clauses and automatic type coercion
- Supports typed tables through ATTypedTableRecursion
- Must execute after AT_PASS_DROP in Phase 2 to see unmodified table state

## Simplified Source

```c
static void
ATPrepAlterColumnType(List **wqueue, AlteredTableInfo *tab, Relation rel,
                     bool recurse, bool recursing, AlterTableCmd *cmd,
                     LOCKMODE lockmode, AlterTableUtilityContext *context)
{
    char *colName = cmd->name;
    ColumnDef *def = (ColumnDef *) cmd->def;
    Node *transform = def->cooked_default;
    HeapTuple tuple;
    Form_pg_attribute attTup;
    AttrNumber attnum;
    Oid targettype;
    int32 targettypmod;
    NewColumnValue *newval;
    ParseState *pstate = make_parsestate(NULL);

    // Check typed table restriction
    if (rel->rd_rel->reloftype && !recursing)
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                       errmsg("cannot alter column type of typed table")));

    // Find and validate the column
    tuple = SearchSysCacheAttName(RelationGetRelid(rel), colName);
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN),
                       errmsg("column \"%s\" of relation \"%s\" does not exist",
                              colName, RelationGetRelationName(rel))));

    attTup = (Form_pg_attribute) GETSTRUCT(tuple);
    attnum = attTup->attnum;

    // Validate column can be altered
    if (attnum <= 0)  // System column check
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                       errmsg("cannot alter system column \"%s\"", colName)));

    if (attTup->attgenerated && def->cooked_default)  // Generated column with USING
        ereport(ERROR, (errcode(ERRCODE_INVALID_COLUMN_DEFINITION),
                       errmsg("cannot specify USING when altering type of generated column")));

    if (attTup->attinhcount > 0 && !recursing)  // Inherited column check
        ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                       errmsg("cannot alter inherited column \"%s\"", colName)));

    // Check partition key restriction
    if (has_partition_attrs(rel, bms_make_singleton(attnum - FirstLowInvalidHeapAttributeNumber), NULL))
        ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                       errmsg("cannot alter column \"%s\" because it is part of the partition key",
                              colName)));

    // Resolve target type and validate permissions
    typenameTypeIdAndMod(NULL, def->typeName, &targettype, &targettypmod);
    if (object_aclcheck(TypeRelationId, targettype, GetUserId(), ACL_USAGE) != ACLCHECK_OK)
        aclcheck_error_type(ACLCHECK_NO_PRIV, targettype);

    // Validate type for column use
    CheckAttributeType(colName, targettype, GetColumnDefCollation(NULL, def, targettype),
                      list_make1_oid(rel->rd_rel->reltype), 0);

    // Handle transformation for regular tables
    if (tab->relkind == RELKIND_RELATION || tab->relkind == RELKIND_PARTITIONED_TABLE) {
        // Create transformation expression (USING clause or default coercion)
        if (!transform) {
            transform = (Node *) makeVar(1, attnum, attTup->atttypid,
                                        attTup->atttypmod, attTup->attcollation, 0);
        }

        // Coerce to target type
        transform = coerce_to_target_type(pstate, transform, exprType(transform),
                                        targettype, targettypmod, COERCION_ASSIGNMENT,
                                        COERCE_IMPLICIT_CAST, -1);

        if (transform == NULL)
            ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                           errmsg("column \"%s\" cannot be cast automatically to type %s",
                                  colName, format_type_be(targettype))));

        // Finalize expression
        assign_expr_collations(pstate, transform);
        transform = (Node *) expression_planner((Expr *) transform);

        // Add to work queue for table rewrite
        newval = (NewColumnValue *) palloc0(sizeof(NewColumnValue));
        newval->attnum = attnum;
        newval->expr = (Expr *) transform;
        newval->is_generated = false;

        tab->newvals = lappend(tab->newvals, newval);
        if (ATColumnChangeRequiresRewrite(transform, attnum))
            tab->rewrite |= AT_REWRITE_COLUMN_REWRITE;
    }

    ReleaseSysCache(tuple);

    // Handle inheritance recursion
    if (recurse) {
        List *child_oids = find_all_inheritors(RelationGetRelid(rel), lockmode, NULL);
        ListCell *lc;

        foreach(lc, child_oids) {
            Oid childrelid = lfirst_oid(lc);
            Relation childrel;

            if (childrelid == RelationGetRelid(rel))
                continue;

            childrel = relation_open(childrelid, NoLock);
            // Validate child column and remap USING expression if needed
            // Queue recursive command
            ATPrepCmd(wqueue, childrel, cmd, false, true, lockmode, context);
            relation_close(childrel, NoLock);
        }
    }

    // Handle typed table recursion
    if (tab->relkind == RELKIND_COMPOSITE_TYPE)
        ATTypedTableRecursion(wqueue, rel, cmd, lockmode, context);
}
```