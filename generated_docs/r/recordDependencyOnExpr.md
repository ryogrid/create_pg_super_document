# recordDependencyOnExpr

## Location
[src/backend/catalog/dependency.c:1553-1595](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/dependency.c#L1553-L1595)

## Overview
Records dependencies between a database object and all objects referenced within an expression tree, enabling proper dependency tracking for rules, constraints, and other expression-based database objects.

## Definition

```c
void
recordDependencyOnExpr(const ObjectAddress *depender,
					   Node *expr, List *rtable,
					   DependencyType behavior)
```
## Detailed Description
This function analyzes an expression or query in node-tree form to identify all database objects it references (tables, columns, operators, functions, etc.) and records dependencies from a specified dependent object to each referenced object. It serves as a high-level interface for dependency recording in expression contexts, commonly used for rules, constraint expressions, triggers, and policies.

The function performs a complete dependency analysis by:
1. Creating a context structure for tracking found references
2. Setting up range table interpretation for variable resolution
3. Walking the expression tree to find all object references
4. Removing duplicate dependencies to optimize storage
5. Recording all unique dependencies in batch

## Parameters / Member Variables
- : Pointer to ObjectAddress of the object that depends on the expression
- : Node tree representing the expression to analyze for dependencies
- : Range table list for interpreting Vars with varlevelsup=0 (can be NIL)
- : Type of dependency to record (normal, auto, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [new_object_addresses](../n/new_object_addresses.md)
  - [find_expr_references_walker](../f/find_expr_references_walker.md)
  - [eliminate_duplicate_dependencies](../e/eliminate_duplicate_dependencies.md)
  - [recordMultipleDependencies](recordMultipleDependencies.md)
  - [free_object_addresses](../f/free_object_addresses.md)
  - list_make1
- Called from (representative examples):
  - [ProcedureCreate](../P/ProcedureCreate.md) (for function expressions)
  - [CreatePolicy](../C/CreatePolicy.md) (for policy expressions)
  - [CreateTriggerFiringOn](../C/CreateTriggerFiringOn.md) (for trigger conditions)
  - [InsertRule](../I/InsertRule.md) (for rule expressions)

## Notes and Other Information
- Used extensively in DDL operations that involve expressions requiring dependency tracking
- Automatically handles duplicate elimination to prevent redundant dependency records
- The rtable parameter enables proper resolution of table references in the expression context
- Memory management is handled internally through object_addresses allocation/deallocation
- Critical for maintaining referential integrity in PostgreSQL's dependency system