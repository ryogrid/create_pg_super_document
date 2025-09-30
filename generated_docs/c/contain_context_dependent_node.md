# contain_context_dependent_node

## Location
[src/backend/optimizer/util/clauses.c:1179-1185](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L1179-L1185)

## Overview
A function that searches for context-dependent nodes within a clause that could cause problems if moved or inlined improperly.

## Definition
```c
static bool contain_context_dependent_node(Node *clause)
```

## Detailed Description
This function identifies nodes that are sensitive to their execution context and could malfunction if moved outside their proper scope. The primary concern is CaseTestExpr nodes, which must appear directly within their corresponding CaseExpr and not be nested inappropriately or moved into different contexts. If such nodes appear "bare" in function arguments, it prevents inlining of SQL functions to avoid creating invalid execution contexts. The function also considers future extensibility for other context-dependent node types like CoerceToDomainValue. It serves as a safety check in the query optimizer to prevent transformations that would break context dependencies.

## Parameters / Member Variables
- `clause`: The expression node tree to search for context-dependent constructs

## Dependencies
- Functions called/Symbols referenced:
  - contain_context_dependent_node_walker
- Called from (representative examples):
  - [inline_function](../i/inline_function.md) (to prevent unsafe function inlining)
  - max_parallel_hazard_context (part of parallelization safety checks)

## Notes and Other Information
- Primarily concerned with CaseTestExpr nodes that must maintain proper relationship to CaseExpr
- Prevents unsafe inlining of SQL functions that could break context dependencies  
- Uses a generic flag-based design to support multiple types of context-dependent nodes
- Critical for maintaining semantic correctness during query transformation
- Located in src/backend/optimizer/util/clauses.c at lines 1179-1185
- Part of PostgreSQL's query optimizer safety infrastructure
- Initializes flags to 0 and delegates actual traversal to the walker function

## Simplified Source

```c
static bool
contain_context_dependent_node(Node *clause)
{
    int flags = 0;

    // Search for context-dependent nodes using walker
    return contain_context_dependent_node_walker(clause, &flags);
}
```