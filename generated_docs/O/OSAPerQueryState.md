# OSAPerQueryState

## Location
[src/backend/utils/adt/orderedsetaggs.c:49-90](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/orderedsetaggs.c#L49-L90)

## Overview
OSAPerQueryState is a structure that holds per-query state data for ordered-set aggregates in PostgreSQL. It contains data and sub-objects that can be created once per query and shared across multiple groups, since they do not change between groups.

## Definition


## Detailed Description
OSAPerQueryState is part of PostgreSQL's generic support for ordered-set aggregates. The structure is designed to optimize memory usage and performance by separating query-level state from group-level state. This per-query state contains information that remains constant across all groups within a single query execution, allowing for efficient resource sharing.

The structure is set up during the first call of the transition function and lives in the executor's per-query memory context. It supports both tuple-based and datum-based accumulation modes, with different sets of fields being used depending on the accumulation strategy.

## Parameters / Member Variables
- : Representative Aggref node for this aggregate function
- : Memory context containing this struct and other per-query data
- : Expression evaluation context for the aggregate
- : Flag indicating if multiple final-function calls are expected within one group

**Tuple accumulation fields:**
- : Tuple descriptor for tuples inserted into the sort state
- : Reusable tuple slot for inserting/extracting tuples
- : Number of sort columns
- : Array of attribute numbers for sort columns
- : Array of sort operator OIDs for each column
- : Array of equality operator OIDs for each column
- : Array of collation OIDs for each column
- : Array of null-ordering flags for each column
- : Compiled expression for tuple equality comparison (created on demand)

**Datum accumulation fields:**
- : Data type OID of datums being sorted
- : Type length for the datum type
- : Whether the type is passed by value
- : Alignment requirement for the type
- : Sort operator OID for datum comparison
- : Equality operator OID for datum comparison
- : Collation OID for datum comparison
- : Null-ordering flag for datum sorting
- : Function manager info for equality function (created on demand)

## Dependencies
- Functions called/Symbols referenced:
  - Aggref
- Called from (representative examples):
  - [OSAPerGroupState](OSAPerGroupState.md) (as a member)
  - [ordered_set_startup](../o/ordered_set_startup.md)

## Notes and Other Information
- This structure is part of PostgreSQL's optimization for ordered-set aggregates, allowing nodeAgg.c to merge aggregates with identical inputs and transition functions
- The per-query state must not depend on the particular aggregate's final function or direct arguments to enable this merging capability
- Memory allocated for this structure is automatically freed at ExecutorEnd()
- The structure supports two different accumulation strategies: tuple-based (for complex sorts) and datum-based (for simple single-column sorts)