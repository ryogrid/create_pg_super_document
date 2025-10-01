# contain_invalid_rfcolumn_walker

## Location
[src/backend/commands/publicationcmds.c:219-257](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/publicationcmds.c#L219-L257)

## Overview
A tree walker function that checks whether any column referenced in a row filter expression is not part of the table's REPLICA IDENTITY.

## Definition

```c
static bool
contain_invalid_rfcolumn_walker(Node *node, rf_context *context)
```
## Detailed Description
This function implements a recursive tree walker that traverses expression nodes to identify Var nodes (column references) and validates whether each referenced column is part of the table's REPLICA IDENTITY. It's specifically designed to validate row filter expressions used in logical replication publications.

The function handles a special case for partitioned tables when publish_via_partition_root is enabled. In this scenario, the row filter is defined on the parent table but needs to be validated against the child table's replica identity. The function performs column name resolution between parent and child tables since their column ordering may differ.

For each Var node encountered, it checks if the column's attribute number (adjusted for heap attribute numbering) is present in the replica identity bitmap. If any column is found that's not part of the replica identity, the function returns true, indicating the row filter contains invalid column references.

## Parameters / Member Variables
- : The expression tree node being examined (can be any Node type)
- : rf_context structure containing validation context including table IDs, replica identity bitmap, and pubviaroot flag

## Dependencies
- Functions called/Symbols referenced:
  - [get_attname](../g/get_attname.md)
  - [get_attnum](../g/get_attnum.md)
  - [bms_is_member](../b/bms_is_member.md)
  - expression_tree_walker
  - FirstLowInvalidHeapAttributeNumber
  - [rf_context](../r/rf_context.md)
- Called from (representative examples):
  - [contain_invalid_rfcolumn_walker](contain_invalid_rfcolumn_walker.md) (recursive)
  - [pub_rf_contains_invalid_column](../p/pub_rf_contains_invalid_column.md)

## Notes and Other Information
- Returns true if any referenced column is NOT in the replica identity, false otherwise
- Handles column mapping between parent and child tables when pubviaroot is enabled
- Uses FirstLowInvalidHeapAttributeNumber offset to adjust attribute numbers for bitmap indexing
- Recursively processes the entire expression tree using expression_tree_walker
- Specifically processes Var nodes while ignoring other node types during traversal
- Located in src/backend/commands/publicationcmds.c:219-257

## Simplified Source

```c
static bool
contain_invalid_rfcolumn_walker(Node *node, rf_context *context)
{
    if (node == NULL)
        return false;

    if (IsA(node, Var))
    {
        Var *var = (Var *) node;
        AttrNumber attnum = var->varattno;

        // Handle column mapping for partitioned tables
        if (context->pubviaroot)
        {
            // Get column name from parent table
            char *colname = get_attname(context->parentid, attnum, false);

            // Find corresponding column number in child table
            attnum = get_attnum(context->relid, colname);
        }

        // Check if column is part of replica identity
        if (!bms_is_member(attnum - FirstLowInvalidHeapAttributeNumber,
                          context->bms_replident))
            return true;  // Invalid column found
    }

    // Recursively check child nodes
    return expression_tree_walker(node, contain_invalid_rfcolumn_walker,
                                 (void *) context);
}
```