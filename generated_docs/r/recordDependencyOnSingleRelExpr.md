# recordDependencyOnSingleRelExpr

## Location
[src/backend/catalog/dependency.c:1596-1697](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/dependency.c#L1596-L1697)

## Overview
Records dependencies between a database object and all objects referenced within an expression tree, with special handling for dependencies on a single specified relation and its columns.

## Definition

```c
void
recordDependencyOnSingleRelExpr(const ObjectAddress *depender,
								Node *expr, Oid relId,
								DependencyType behavior,
								DependencyType self_behavior,
								bool reverse_self)
```
## Detailed Description
This function is a specialized version of recordDependencyOnExpr designed for expressions that reference only one relation (with varno = 1 and varlevelsup = 0). It provides enhanced control over how dependencies are recorded for the target relation versus other referenced objects, with options for different dependency types and directional control.

The function creates a minimal range table entry for the specified relation and processes the expression tree to find all references. It then separates dependencies into two categories: self-dependencies (references to the specified relation and its columns) and external dependencies (references to other objects), applying different dependency types and potentially reversing the direction for self-dependencies.

Key features:
- Handles single-relation expressions efficiently
- Supports different dependency types for self vs. external references  
- Can reverse dependency direction for column references
- Automatically eliminates duplicate dependencies

## Parameters / Member Variables
- : Pointer to ObjectAddress of the object that depends on the expression
- : Node tree representing the expression to analyze
- : OID of the single relation expected to be referenced in the expression
- : Dependency type for external object references
- : Dependency type for references to the specified relation and its columns
- : If true, column dependencies are reversed (columns depend on table, not vice versa)

## Dependencies
- Functions called/Symbols referenced:
  - [new_object_addresses](../n/new_object_addresses.md)
  - [find_expr_references_walker](../f/find_expr_references_walker.md)
  - [eliminate_duplicate_dependencies](../e/eliminate_duplicate_dependencies.md)
  - [add_exact_object_address](../a/add_exact_object_address.md)
  - [recordMultipleDependencies](recordMultipleDependencies.md)
  - [recordDependencyOn](recordDependencyOn.md)
  - [free_object_addresses](../f/free_object_addresses.md)
  - list_make1
- Called from (representative examples):
  - [StorePartitionKey](../S/StorePartitionKey.md) (for partition key expressions)
  - index_create (for index expressions)
  - [StoreAttrDefault](../S/StoreAttrDefault.md) (for column default expressions)
  - [CreateConstraintEntry](../C/CreateConstraintEntry.md) (for constraint expressions)

## Notes and Other Information
- The caller should ensure a whole-table dependency is created separately if needed
- Whole-row Var references (relation.*) do not generate dependency items
- Uses a 'bogus' range table entry with minimal required fields for Var resolution
- The reverse_self option is useful for cases where columns should depend on their containing table
- Memory management handles both regular and self-dependency address collections
- Critical for DDL operations involving single-table expressions like defaults, constraints, and indexes