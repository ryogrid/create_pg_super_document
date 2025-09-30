# contain_leaked_vars

## Location
[src/backend/optimizer/util/clauses.c:1263-1268](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L1263-L1268)

## Overview
The `contain_leaked_vars` function recursively scans a clause to discover whether it contains any Var nodes (of the current query level) that are passed as arguments to leaky functions that might leak sensitive data.

## Definition
```c
bool contain_leaked_vars(Node *clause)
```

## Detailed Description
This function serves as the main entry point for checking whether a clause contains variables that could be leaked through non-leakproof functions. It is part of PostgreSQL's security barrier mechanism that prevents data leakage in queries with security-sensitive predicates. The function acts as a wrapper that calls the actual tree-walking implementation (`contain_leaked_vars_walker`) to perform the recursive analysis.

The function is crucial for determining the order of predicate evaluation in queries where security barriers are involved. Clauses that contain leaked vars must be applied after any lower-level security barrier clauses to prevent unauthorized data access.

## Parameters / Member Variables
- `clause`: A Node pointer representing the clause (expression tree) to be analyzed for potential variable leakage

## Dependencies
- Functions called/Symbols referenced:
  - [contain_leaked_vars_walker](contain_leaked_vars_walker.md)
- Called from (representative examples):
  - [qual_is_pushdown_safe](../q/qual_is_pushdown_safe.md)
  - [make_restrictinfo_internal](../m/make_restrictinfo_internal.md)
  - [WindowFuncLists](../W/WindowFuncLists.md)

## Notes and Other Information
- Returns true if the clause contains any non-leakproof functions that are passed Var nodes of the current query level
- This function is essential for PostgreSQL's row-level security (RLS) and security barrier views implementation
- The actual logic is delegated to `contain_leaked_vars_walker` which performs the tree traversal
- Located in src/backend/optimizer/util/clauses.c:1263-1268

## Simplified Source

```c
bool
contain_leaked_vars(Node *clause)
{
    // Simple wrapper that delegates to the actual tree-walking function
    return contain_leaked_vars_walker(clause, NULL);
}
```