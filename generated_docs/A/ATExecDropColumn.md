# ATExecDropColumn

## Location
[src/backend/commands/tablecmds.c:8978-9178](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L8978-L9178)

## Overview
ATExecDropColumn implements the execution phase of ALTER TABLE DROP COLUMN, handling the complex logic of dropping columns from tables while managing inheritance hierarchies, partition constraints, and cascading effects.

## Definition

```c
static ObjectAddress
ATExecDropColumn(List **wqueue, Relation rel, const char *colName,
				 DropBehavior behavior,
				 bool recurse, bool recursing,
				 bool missing_ok, LOCKMODE lockmode,
				 ObjectAddresses *addrs)
```
## Detailed Description
This function orchestrates the dropping of a column from a relation, handling complex scenarios including inheritance hierarchies, partitioned tables, and system constraints. It performs extensive validation (system columns, inherited columns, partition key usage), manages recursive descent through child relations, and coordinates the final deletion through PostgreSQL's dependency system. The function uses a two-phase approach: collecting objects to delete during recursion, then performing all deletions atomically at the top level.

The function is recursive and handles different behaviors for inheritance children based on whether they have local definitions or are purely inherited.

## Parameters / Member Variables
- `wqueue`: Work queue for storing additional ALTER TABLE commands
- `rel`: The relation (table) from which to drop the column
- `colName`: Name of the column to be dropped
- `behavior`: Drop behavior (CASCADE or RESTRICT) for handling dependencies
- `recurse`: Whether to recurse through inheritance hierarchy
- `recursing`: Flag indicating if this is a recursive call
- `missing_ok`: Whether to emit notice instead of error if column doesn't exist
- `lockmode`: Lock mode for accessing child relations
- `addrs`: Collection of object addresses to delete (used in recursion)

## Dependencies
- Functions called/Symbols referenced:
  - [ATSimplePermissions](ATSimplePermissions.md)
  - [check_stack_depth](../c/check_stack_depth.md)
  - [new_object_addresses](../n/new_object_addresses.md)
  - [SearchSysCacheAttName](../S/SearchSysCacheAttName.md)
  - [SearchSysCacheCopyAttName](../S/SearchSysCacheCopyAttName.md)
  - [has_partition_attrs](../h/has_partition_attrs.md)
  - [find_inheritance_children](../f/find_inheritance_children.md)
  - [CheckAlterTableIsSafe](../C/CheckAlterTableIsSafe.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md)
  - [add_exact_object_address](../a/add_exact_object_address.md)
  - [performMultipleDeletions](../p/performMultipleDeletions.md)
  - [free_object_addresses](../f/free_object_addresses.md)
  - [heap_freetuple](../h/heap_freetuple.md)
- Called from (representative examples):
  - [ATExecCmd](ATExecCmd.md)
  - [ATExecDropColumn](ATExecDropColumn.md) (recursive)
  - child_dependency_type

## Notes and Other Information
- Located in src/backend/commands/tablecmds.c:8978-9178
- Returns ObjectAddress of the dropped column
- Validates against dropping system columns (attnum <= 0)
- Prevents dropping inherited columns unless recursing from parent
- Prevents dropping partition key columns to avoid cascaded table deletion
- Handles partitioned tables by requiring explicit recursion to child partitions
- Uses inheritance count management for child relations (decrement vs. delete)
- Implements stack overflow protection due to recursive nature
- Atomic deletion of all collected objects at top-level completion
- Sets attislocal=true for child columns when parent column dropped without recursion

## Simplified Source

```c
static ObjectAddress
ATExecDropColumn(List **wqueue, Relation rel, const char *colName,
                 DropBehavior behavior, bool recurse, bool recursing,
                 bool missing_ok, LOCKMODE lockmode,
                 ObjectAddresses *addrs)
{
    HeapTuple tuple;
    Form_pg_attribute targetatt;
    AttrNumber attnum;
    List *children;
    ObjectAddress object;

    // Check permissions for recursive calls
    if (recursing)
        ATSimplePermissions(AT_DropColumn, rel, ATT_TABLE | ATT_FOREIGN_TABLE);

    // Initialize object address collection for top-level call
    if (!recursing)
        addrs = new_object_addresses();

    // Find the column to drop
    tuple = SearchSysCacheAttName(RelationGetRelid(rel), colName);
    if (!HeapTupleIsValid(tuple))
    {
        if (missing_ok)
        {
            ereport(NOTICE, (errmsg("column \"%s\" does not exist, skipping", colName)));
            return InvalidObjectAddress;
        }
        ereport(ERROR, (errmsg("column \"%s\" of relation \"%s\" does not exist",
                               colName, RelationGetRelationName(rel))));
    }

    targetatt = (Form_pg_attribute) GETSTRUCT(tuple);
    attnum = targetatt->attnum;

    // Validate column can be dropped
    if (attnum <= 0)
        ereport(ERROR, (errmsg("cannot drop system column \"%s\"", colName)));

    if (targetatt->attinhcount > 0 && !recursing)
        ereport(ERROR, (errmsg("cannot drop inherited column \"%s\"", colName)));

    // Check if column is part of partition key
    bool is_expr;
    if (has_partition_attrs(rel, bms_make_singleton(attnum - FirstLowInvalidHeapAttributeNumber), &is_expr))
        ereport(ERROR, (errmsg("cannot drop partition key column \"%s\"", colName)));

    ReleaseSysCache(tuple);

    // Handle inheritance children
    children = find_inheritance_children(RelationGetRelid(rel), lockmode);
    if (children)
    {
        // For partitioned tables, require explicit recursion
        if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE && !recurse)
            ereport(ERROR, (errmsg("cannot drop column from only the partitioned table when partitions exist")));

        // Process each child relation
        Relation attr_rel = table_open(AttributeRelationId, RowExclusiveLock);
        ListCell *child;
        foreach(child, children)
        {
            Oid childrelid = lfirst_oid(child);
            Relation childrel = table_open(childrelid, NoLock);

            // Get child column info
            HeapTuple child_tuple = SearchSysCacheCopyAttName(childrelid, colName);
            Form_pg_attribute childatt = (Form_pg_attribute) GETSTRUCT(child_tuple);

            if (recurse)
            {
                // Either drop child column or decrement inheritance count
                if (childatt->attinhcount == 1 && !childatt->attislocal)
                {
                    // Recursively drop from child
                    ATExecDropColumn(wqueue, childrel, colName, behavior, true, true,
                                     false, lockmode, addrs);
                }
                else
                {
                    // Just decrement inheritance count
                    childatt->attinhcount--;
                    CatalogTupleUpdate(attr_rel, &child_tuple->t_self, child_tuple);
                }
            }
            else
            {
                // Mark as local definition
                childatt->attinhcount--;
                childatt->attislocal = true;
                CatalogTupleUpdate(attr_rel, &child_tuple->t_self, child_tuple);
            }

            heap_freetuple(child_tuple);
            table_close(childrel, NoLock);
        }
        table_close(attr_rel, RowExclusiveLock);
    }

    // Add column to deletion list
    object.classId = RelationRelationId;
    object.objectId = RelationGetRelid(rel);
    object.objectSubId = attnum;
    add_exact_object_address(&object, addrs);

    // At top level, perform all deletions
    if (!recursing)
    {
        performMultipleDeletions(addrs, behavior, 0);
        free_object_addresses(addrs);
    }

    return object;
}
```