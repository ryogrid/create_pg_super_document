# ATExecSetRelOptions

## Location
[src/backend/commands/tablecmds.c:15049-15252](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L15049-L15252)

## Overview
ATExecSetRelOptions executes the ALTER TABLE SET/RESET/REPLACE relation options commands, updating storage parameters and configuration options for database relations and their associated TOAST tables.

## Definition
```c
static void ATExecSetRelOptions(Relation rel, List *defList, AlterTableType operation, LOCKMODE lockmode)
```

## Detailed Description
This function implements the execution phase for ALTER TABLE commands that modify relation options (reloptions). It handles three types of operations: setting new options, resetting options to defaults, and completely replacing the options list. The function validates the new options based on the relation kind (table, view, index, etc.), updates the pg_class system catalog, and also processes any associated TOAST table.

The function follows a comprehensive workflow: it retrieves existing options, transforms the new option list, validates the options against the relation type, updates the system catalog, and handles TOAST table options separately. Special validation is performed for views with CHECK OPTION to ensure they are auto-updatable.

## Parameters / Member Variables
- `rel`: The relation being modified  
- `defList`: List of DefElem structures containing the new option definitions
- `operation`: Type of operation (AT_SetRelOptions, AT_ResetRelOptions, or AT_ReplaceRelOptions)
- `lockmode`: Lock mode for accessing related objects

## Dependencies
- Functions called/Symbols referenced:
  - [transformRelOptions](../t/transformRelOptions.md): Processes and validates the option list
  - [heap_reloptions](../h/heap_reloptions.md): Validates options for heap tables
  - [partitioned_table_reloptions](../p/partitioned_table_reloptions.md): Validates options for partitioned tables  
  - [view_reloptions](../v/view_reloptions.md): Validates options for views
  - [index_reloptions](../i/index_reloptions.md): Validates options for indexes
  - [get_view_query](../g/get_view_query.md): Retrieves the query definition for views
  - [view_query_is_auto_updatable](../v/view_query_is_auto_updatable.md): Checks if view supports CHECK OPTION
  - [SearchSysCacheLocked1](../S/SearchSysCacheLocked1.md): Looks up relation tuple in system cache
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md): Updates the pg_class system catalog
  - [heap_modify_tuple](../h/heap_modify_tuple.md): Creates modified version of heap tuple
  - InvokeObjectPostAlterHook: Triggers post-alter hooks
  - [errdetail_relkind_not_supported](../e/errdetail_relkind_not_supported.md): Generates error details for unsupported relation kinds

- Called from (representative examples):
  - [ATExecCmd](ATExecCmd.md): Main ALTER TABLE command execution dispatcher

## Notes and Other Information
- Supports different relation kinds: regular tables, partitioned tables, materialized views, views, indexes, and TOAST tables
- Automatically handles TOAST table option updates when modifying the main table
- Performs special validation for views with CHECK OPTION to ensure auto-updatability
- Uses relation-specific validation functions based on the relation kind
- Updates are propagated to relation caches during post-commit cache invalidation
- Handles three operation types: setting new options, resetting to defaults, and complete replacement
- Maintains transactional safety by using appropriate locking and system catalog updates

## Simplified Source

```c
static void
ATExecSetRelOptions(Relation rel, List *defList, AlterTableType operation,
                    LOCKMODE lockmode)
{
    Oid relid;
    Relation pgclass;
    HeapTuple tuple, newtuple;
    Datum datum, newOptions;
    bool isnull;
    Datum repl_val[Natts_pg_class];
    bool repl_null[Natts_pg_class];
    bool repl_repl[Natts_pg_class];
    static char *validnsps[] = HEAP_RELOPT_NAMESPACES;

    if (defList == NIL && operation != AT_ReplaceRelOptions)
        return;  // Nothing to do

    // Open pg_class and get relation tuple
    pgclass = table_open(RelationRelationId, RowExclusiveLock);
    relid = RelationGetRelid(rel);
    tuple = SearchSysCacheLocked1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(tuple))
        elog(ERROR, "cache lookup failed for relation %u", relid);

    // Get existing options or start fresh for REPLACE operation
    if (operation == AT_ReplaceRelOptions)
    {
        datum = (Datum) 0;
        isnull = true;
    }
    else
    {
        datum = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_reloptions, &isnull);
    }

    // Generate new options
    newOptions = transformRelOptions(isnull ? (Datum) 0 : datum,
                                     defList, NULL, validnsps, false,
                                     operation == AT_ResetRelOptions);

    // Validate options based on relation kind
    switch (rel->rd_rel->relkind)
    {
        case RELKIND_RELATION:
        case RELKIND_TOASTVALUE:
        case RELKIND_MATVIEW:
            (void) heap_reloptions(rel->rd_rel->relkind, newOptions, true);
            break;
        case RELKIND_PARTITIONED_TABLE:
            (void) partitioned_table_reloptions(newOptions, true);
            break;
        case RELKIND_VIEW:
            (void) view_reloptions(newOptions, true);
            break;
        case RELKIND_INDEX:
        case RELKIND_PARTITIONED_INDEX:
            (void) index_reloptions(rel->rd_indam->amoptions, newOptions, true);
            break;
        default:
            ereport(ERROR, "cannot set options for this relation type");
            break;
    }

    // Special validation for views with CHECK OPTION
    if (rel->rd_rel->relkind == RELKIND_VIEW)
    {
        Query *view_query = get_view_query(rel);
        List *view_options = untransformRelOptions(newOptions);
        bool check_option = false;

        foreach(cell, view_options)
        {
            DefElem *defel = (DefElem *) lfirst(cell);
            if (strcmp(defel->defname, "check_option") == 0)
                check_option = true;
        }

        if (check_option)
        {
            const char *view_updatable_error =
                view_query_is_auto_updatable(view_query, true);
            if (view_updatable_error)
                ereport(ERROR, "WITH CHECK OPTION is supported only on automatically updatable views");
        }
    }

    // Update pg_class tuple
    memset(repl_val, 0, sizeof(repl_val));
    memset(repl_null, false, sizeof(repl_null));
    memset(repl_repl, false, sizeof(repl_repl));

    if (newOptions != (Datum) 0)
        repl_val[Anum_pg_class_reloptions - 1] = newOptions;
    else
        repl_null[Anum_pg_class_reloptions - 1] = true;
    repl_repl[Anum_pg_class_reloptions - 1] = true;

    newtuple = heap_modify_tuple(tuple, RelationGetDescr(pgclass),
                                 repl_val, repl_null, repl_repl);
    CatalogTupleUpdate(pgclass, &newtuple->t_self, newtuple);
    UnlockTuple(pgclass, &tuple->t_self, InplaceUpdateTupleLock);

    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), 0);
    heap_freetuple(newtuple);
    ReleaseSysCache(tuple);

    // Handle TOAST table if it exists (abbreviated version)
    if (OidIsValid(rel->rd_rel->reltoastrelid))
    {
        // Similar process for TOAST table (omitted for brevity)
    }

    table_close(pgclass, RowExclusiveLock);
}
```