# show_upper_qual

## Location
[src/backend/commands/explain.c:2545-2558](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L2545-L2558)

## Overview
A specialized wrapper function for displaying qualifier expressions in upper-level plan nodes, determining prefix usage based on the complexity of the query's range table and verbosity settings.

## Definition
```c
static void show_upper_qual(List *qual, const char *qlabel,
                           PlanState *planstate, List *ancestors,
                           ExplainState *es)
```

## Detailed Description
The `show_upper_qual` function is designed specifically for displaying qualification expressions in upper-level plan nodes such as joins, aggregates, sorts, and other non-scan operations. Unlike `show_scan_qual`, which focuses on scan-specific prefix logic, this function determines prefix usage based on the overall complexity of the query rather than the specific node type.

The function implements a range table-based prefixing strategy: it uses table prefixes when the query involves multiple tables (range table length > 1) or when verbose output is requested. This approach is particularly appropriate for upper-level nodes because they typically operate on data from multiple sources, making column reference ambiguity more likely. After making the prefix decision, it delegates to `show_qual` for the actual formatting.

## Parameters / Member Variables
- `qual`: List of qualification expressions with implicit AND semantics to be displayed
- `qlabel`: The label to use when displaying this qualification in the EXPLAIN output
- `planstate`: The plan state containing execution context for the current upper-level node
- `ancestors`: List of ancestor plan nodes providing context for variable resolution
- `es`: The ExplainState structure containing range table information and verbosity settings

## Dependencies
- Functions called/Symbols referenced:
  - [show_qual](show_qual.md)
  - [list_length](../l/list_length.md) (macro to check range table size)
- Called from (representative examples):
  - [ExplainNode](../E/ExplainNode.md) (for join, aggregate, sort, and other upper-level nodes)
  - [show_modifytable_info](show_modifytable_info.md)

## Notes and Other Information
- Uses range table complexity as the primary criterion for prefix decisions, unlike show_scan_qual which uses node type
- Particularly useful for join conditions, HAVING clauses, and other upper-level filtering operations
- The range table length check (> 1) effectively identifies multi-table queries where prefixes add clarity
- Part of the specialized EXPLAIN infrastructure, complementing show_scan_qual for different plan node categories
- Frequently used throughout ExplainNode for various upper-level operations like Hash Join, Merge Join, Group, etc.

## Simplified Source

```c
static void show_upper_qual(List *qual, const char *qlabel,
                           PlanState *planstate, List *ancestors,
                           ExplainState *es) {
    // Determine if table prefixes should be used
    // Use prefixes for multi-table queries or when verbose output requested
    bool useprefix = (list_length(es->rtable) > 1 || es->verbose);

    // Delegate to general qualification display function
    show_qual(qual, qlabel, planstate, ancestors, useprefix, es);
}
```