# acquire_inherited_sample_rows

## Location
[src/backend/commands/analyze.c:1345-1608](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/analyze.c#L1345-L1608)

## Overview
Acquires sample rows from an inheritance tree by collecting samples proportionally from all inheritance children, handling tuple conversion between different table structures as needed.

## Definition

```c
static int
acquire_inherited_sample_rows(Relation onerel, int elevel,
							  HeapTuple *rows, int targrows,
							  double *totalrows, double *totaldeadrows)
```
## Detailed Description
The acquire_inherited_sample_rows function extends the sampling capability to inheritance hierarchies, collecting rows from all tables in an inheritance tree rather than just a single table. It discovers all inheritance children using find_all_inheritors, then samples from each child proportionally to its block count relative to the total blocks across all children.

The function handles several complex scenarios: it validates that analyzable children exist, manages different table types (regular tables, foreign tables, materialized views), and performs tuple conversion when child tables have different column structures than the parent. For foreign tables, it consults the Foreign Data Wrapper (FDW) to determine if analysis is supported.

Sampling is distributed proportionally based on each child's block count, ensuring that larger child tables contribute more samples. When child table schemas differ from the parent, the function converts tuples using column name matching to maintain compatibility.

## Parameters / Member Variables
- : The parent relation of the inheritance tree
- : Error reporting level for progress messages
- : Caller-allocated array to store sampled tuples from all children
- : Target total number of rows to sample across all children
- : Output parameter for estimated total live rows across all children
- : Output parameter for estimated total dead rows across all children

## Dependencies
- Functions called/Symbols referenced:
  - [find_all_inheritors](../f/find_all_inheritors.md)
  - [acquire_sample_rows](acquire_sample_rows.md)
  - CommandCounterIncrement
  - [SetRelationHasSubclass](../S/SetRelationHasSubclass.md)
  - [GetFdwRoutineForRelation](../G/GetFdwRoutineForRelation.md)
  - [equalRowTypes](../e/equalRowTypes.md)
  - [convert_tuples_by_name](../c/convert_tuples_by_name.md)
  - [execute_attr_map_tuple](../e/execute_attr_map_tuple.md)
  - [free_conversion_map](../f/free_conversion_map.md)
  - [heap_freetuple](../h/heap_freetuple.md)
- Called from (representative examples):
  - [do_analyze_rel](../d/do_analyze_rel.md)

## Notes and Other Information
- Fails if no analyzable child tables exist in the inheritance hierarchy
- Handles foreign tables by consulting their FDW analyze hooks
- Performs automatic tuple conversion when child schemas differ from parent schema
- Distributes sample size proportionally based on relative block counts of children
- Updates relhassubclass catalog flag if no children are found
- Maintains table locks on children to preserve TOAST table references
- Provides detailed progress reporting for multi-child table analysis
- Ignores temp tables from other backends and non-analyzable table types