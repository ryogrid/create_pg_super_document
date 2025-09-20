# PredClass

## Location
[src/backend/optimizer/util/predtest.c:55-56](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/predtest.c#L55-L56)

## Overview
PredClass is an enumeration type that classifies expression nodes based on their logical structure for predicate testing operations in PostgreSQL's optimizer.

## Definition

```c
typedef struct PredIterInfoData *PredIterInfo;
```
## Detailed Description
PredClass is used in PostgreSQL's predicate testing framework to categorize expressions according to their logical properties. This classification enables the optimizer to handle different types of logical expressions uniformly when performing operations such as predicate implication and refutation testing. The enum helps abstract away the specific node types and focuses on the logical semantics (atomic, conjunction, or disjunction) of expressions.

The classification is essential for iterating over components of complex logical expressions without having to handle each specific expression node type separately. This abstraction simplifies the code that processes predicates during query optimization.

## Parameters / Member Variables
- : Represents expressions that are neither AND nor OR operations - these are atomic predicates or leaf nodes in the logical expression tree
- : Represents expressions with AND semantics, indicating conjunctive operations where all components must be true
- : Represents expressions with OR semantics, indicating disjunctive operations where at least one component must be true

## Dependencies
- Functions called/Symbols referenced: None (enum definition)
- Called from (representative examples):
  - iterate_end (src/backend/optimizer/util/predtest.c:87)
  - [predicate_implied_by_recurse](../p/predicate_implied_by_recurse.md) (src/backend/optimizer/util/predtest.c:295)
  - [predicate_refuted_by_recurse](../p/predicate_refuted_by_recurse.md) (src/backend/optimizer/util/predtest.c:536, 825)

## Notes and Other Information
This enum is part of PostgreSQL's predicate testing infrastructure used by the query optimizer. It works in conjunction with the PredIterInfo framework to provide a generic way to iterate over and analyze complex logical expressions. The classification allows the optimizer to apply different logical reasoning rules based on whether an expression represents an atomic condition, a conjunction (AND), or a disjunction (OR) of sub-expressions.