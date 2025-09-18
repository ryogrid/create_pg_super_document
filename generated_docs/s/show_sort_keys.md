# show_sort_keys

## Location
src/backend/commands/explain.c: 2559 - 2573

## Overview
A specialized function for displaying the sort keys and ordering information for Sort plan nodes in PostgreSQL EXPLAIN output.

## Definition
```c
static void show_sort_keys(SortState *sortstate, List *ancestors, ExplainState *es)
```

## Detailed Description
The `show_sort_keys` function is responsible for displaying the sorting criteria used by Sort plan nodes in PostgreSQL query execution plans. It extracts the sort-related information from the Sort plan node and delegates to the more general `show_sort_group_keys` function to format and display the complete sorting specification.

This function serves as a specialized adapter that converts Sort node data structures into the parameters needed by the general-purpose sort/group key display function. It extracts key information including the number of sort columns, column indices, sort operators, collations, and null ordering preferences, then passes this information along with appropriate labeling to create readable EXPLAIN output.

The function is part of PostgreSQL's comprehensive EXPLAIN system, specifically handling the display of ORDER BY clauses and other sorting operations within query execution plans.

## Parameters / Member Variables
- `sortstate`: The SortState containing the execution state and plan information for the sort operation
- `ancestors`: List of ancestor plan nodes providing context for variable resolution
- `es`: The ExplainState structure containing output formatting options and accumulated results

## Dependencies
- Functions called/Symbols referenced:
  - show_sort_group_keys (delegates the actual formatting work)
  - Sort (casts to access the plan structure)
- Called from (representative examples):
  - ExplainNode (specifically for Sort node types)

## Notes and Other Information
- Acts as a thin wrapper around show_sort_group_keys, providing Sort-specific parameter extraction
- Accesses the Sort plan structure through the SortState to get sorting specifications
- Uses "Sort Key" as the standard label for sort key display in EXPLAIN output
- The function demonstrates PostgreSQL's layered approach to EXPLAIN formatting, with specialized functions handling specific node types
- Part of the broader sort and group key display infrastructure, complementing similar functions for other grouping and ordering operations
- Critical for helping users understand query performance by showing how data is being ordered