# show_qual

## Location
[src/backend/commands/explain.c:2510-2530](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L2510-L2530)

## Overview
A static helper function that displays qualifier expressions (WHERE/JOIN conditions) in PostgreSQL EXPLAIN output, converting implicit AND lists into explicit AND expressions for readable display.

## Definition
```c
static void show_qual(List *qual, const char *qlabel,
                     PlanState *planstate, List *ancestors,
                     bool useprefix, ExplainState *es)
```

## Detailed Description
The `show_qual` function handles the display of qualifier expressions in PostgreSQL's query execution plans. In PostgreSQL's internal representation, multiple conditions are stored as a list with implicit AND semantics (meaning all conditions must be true). This function converts that implicit AND list into an explicit AND expression tree and then delegates to `show_expression` for the actual formatting and display.

The function serves as a specialized wrapper around `show_expression` that handles the common case of displaying qualification conditions like WHERE clauses, JOIN conditions, and filter expressions that are internally represented as lists of conditions.

## Parameters / Member Variables
- `qual`: List of qualification expressions with implicit AND semantics (NULL if no qualifications)
- `qlabel`: The label to use when displaying this qualification in the EXPLAIN output
- `planstate`: The plan state containing execution context for the current plan node
- `ancestors`: List of ancestor plan nodes providing context for variable resolution
- `useprefix`: Boolean flag indicating whether to use table prefixes in column references
- `es`: The ExplainState structure containing output formatting options and accumulated results

## Dependencies
- Functions called/Symbols referenced:
  - [make_ands_explicit](../m/make_ands_explicit.md)
  - [show_expression](show_expression.md)
- Called from (representative examples):
  - [show_scan_qual](show_scan_qual.md)
  - [show_upper_qual](show_upper_qual.md)

## Notes and Other Information
- Returns early if the qualification list is empty (NIL), avoiding unnecessary processing
- The conversion from implicit AND list to explicit AND tree is crucial for proper display formatting
- This function bridges the gap between PostgreSQL's internal list-based representation and user-friendly tree-based display
- Part of the hierarchical structure of EXPLAIN formatting functions, with show_qual being more specialized than show_expression

## Simplified Source

```c
static void show_qual(List *qual, const char *qlabel,
                     PlanState *planstate, List *ancestors,
                     bool useprefix, ExplainState *es) {
    // Early return if no qualifications to display
    if (qual == NIL)
        return;

    // Convert implicit AND list to explicit AND expression
    Node *node = (Node *) make_ands_explicit(qual);

    // Display the formatted expression
    show_expression(node, qlabel, planstate, ancestors, useprefix, es);
}
```