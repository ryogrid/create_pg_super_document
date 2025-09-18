# range_table_walker_impl

## Location
src/backend/nodes/nodeFuncs.c: 2789 - 2809

## Overview
This function walks through a query's range table, visiting each RangeTblEntry with a user-provided walker callback function.

## Definition
```c
bool range_table_walker_impl(List *rtable,
                            tree_walker_callback walker,
                            void *context,
                            int flags)
```

## Detailed Description
The `range_table_walker_impl` function is a specialized walker that iterates through a PostgreSQL query's range table (rtable). It is extracted as a separate function from `query_tree_walker_impl` to allow independent traversal of range tables when needed. The function processes each RangeTblEntry in the list, calling `range_table_entry_walker` for each entry to perform the actual node traversal.

This function serves as a building block for more comprehensive query traversals and can be useful on its own when only range table processing is required, such as during dependency analysis or security checks that need to examine table references.

## Parameters / Member Variables
- `rtable`: List of RangeTblEntry nodes representing the query's range table
- `walker`: Callback function of type tree_walker_callback that will be called for nodes within each range table entry
- `context`: User-defined context data passed to the walker callbacks
- `flags`: Bitwise OR of flag values controlling traversal behavior (passed through to range_table_entry_walker)

## Dependencies
- Functions called/Symbols referenced:
  - range_table_entry_walker
  - RangeTblEntry (node type)
  - List (PostgreSQL list structure)
- Called from (representative examples):
  - range_table_walker (inline wrapper)
  - query_tree_walker_impl
  - planstate_tree_walker

## Notes and Other Information
- This function is split out from query_tree_walker for modularity and reusability
- Returns true if any walker callback returns true (early termination)
- Uses PostgreSQL's foreach macro for efficient list iteration
- The actual node traversal logic is delegated to range_table_entry_walker for each individual range table entry
- Supports the same flag-based traversal control as other walker functions in the system