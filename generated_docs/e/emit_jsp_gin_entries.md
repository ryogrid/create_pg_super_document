# emit_jsp_gin_entries

## Location
src/backend/utils/adt/jsonb_gin.c: 719 - 747

## Overview
Recursively traverses a JsonPathGinNode tree and collects all GIN index entries, replacing entry datums with their corresponding indices in the GIN entries array.

## Definition


## Detailed Description
This function performs a depth-first traversal of a JsonPathGinNode tree to collect and register all GIN index entries required for query execution. It serves as the final phase in jsonpath GIN query preparation, converting the abstract query tree into concrete GIN entries that can be used by the index access method.

For leaf nodes (JSP_GIN_ENTRY), the function calls  to register the entry datum with the GIN entries collection and replaces the datum with its index in the entries array. For branch nodes (JSP_GIN_OR, JSP_GIN_AND), it recursively processes all child nodes to ensure all entries in the subtree are collected.

The function modifies the node tree in-place by replacing entry datums with their indices, which are subsequently used by the query execution engine to reference the actual GIN entries.

## Parameters / Member Variables
- : JsonPathGinNode pointer to the current node being processed in the query tree
- : GinEntries collection that accumulates all unique GIN entries discovered during traversal

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth (prevents stack overflow during deep recursion)
  - add_gin_entry (registers a GIN entry and returns its index)
  - emit_jsp_gin_entries (recursive self-calls for tree traversal)
- Called from (representative examples):
  - extract_jsp_query (main query extraction and entry collection)
  - emit_jsp_gin_entries (recursive self-calls for child nodes)

## Notes and Other Information
- The function operates in-place, modifying the node tree structure during traversal
- Only processes JSP_GIN_ENTRY, JSP_GIN_OR, and JSP_GIN_AND node types
- Uses recursive descent with stack depth checking to handle arbitrarily deep expression trees
- The entry index replacement is crucial for subsequent query execution phases
- Does not handle other node types, suggesting they are processed elsewhere or represent invalid states