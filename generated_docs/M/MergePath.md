# MergePath

## Location
src/include/nodes/pathnodes.h: 2132 - 2140

## Overview
MergePath represents a merge join algorithm path that can efficiently join two sorted inputs by merging them in order, potentially incorporating multiple execution nodes.

## Definition
```c
typedef struct MergePath
{
    JoinPath    jpath;
    List       *path_mergeclauses;  /* join clauses to be used for merge */
    List       *outersortkeys;      /* keys for explicit sort, if any */
    List       *innersortkeys;      /* keys for explicit sort, if any */
    bool        skip_mark_restore;  /* can executor skip mark/restore? */
    bool        materialize_inner;  /* add Materialize to inner? */
} MergePath;
```

## Detailed Description
MergePath represents one of the most sophisticated join algorithms in PostgreSQL. Unlike other path types that represent a single runtime plan node, a MergePath can represent up to four different execution nodes: the MergeJoin node itself, optional Sort nodes for outer and inner inputs, and/or a Material node for the inner input. This complex structure is represented as a single path node to avoid palloc overhead during the extensive path exploration that occurs in complex join problems.

The merge join algorithm works by simultaneously scanning two sorted inputs and matching tuples based on join keys. It is particularly efficient for large datasets when both inputs are already sorted or can be efficiently sorted. The algorithm requires that join conditions be mergeable (typically equality conditions on sortable data types).

The path contains detailed information about sorting requirements, optimization flags for mark/restore operations, and materialization needs, making it a comprehensive representation of merge join execution strategy.

## Parameters / Member Variables
- `jpath`: Base JoinPath structure containing standard join information
- `path_mergeclauses`: List of RestrictInfo structures representing join clauses that will be used for the merge operation (subset of parent relations restriction clauses)
- `outersortkeys`: PathKeys list describing required ordering for outer input, or NIL if already appropriately sorted
- `innersortkeys`: PathKeys list describing required ordering for inner input, or NIL if already appropriately sorted
- `skip_mark_restore`: Boolean flag indicating whether executor can skip mark/restore calls (optimization when only one match per outer tuple is needed)
- `materialize_inner`: Boolean flag indicating whether a Material node should be added atop the inner input to allow rescanning

## Dependencies
- Functions called/Symbols referenced:
  - JoinPath (base structure)
  - List (PostgreSQL list structure)
  - RestrictInfo (via path_mergeclauses)
  - PathKeys (via sort key lists)

- Called from (representative examples):
  - GetExistingLocalJoinPath (foreign data wrapper support)
  - final_cost_mergejoin (merge join specific cost calculation)
  - create_mergejoin_plan (converts path to execution plan)
  - create_mergejoin_path (creates new MergePath instances)
  - create_nestloop_path (when considering join alternatives)

## Notes and Other Information
- Merge join requires mergeable join clauses (typically equality conditions on sortable types)
- Can be very efficient for large datasets, especially when inputs are pre-sorted or sorting cost is amortized
- The skip_mark_restore optimization applies when each outer tuple matches at most one inner tuple and mergeclauses are sufficient for identification
- Materialization of inner input may be necessary when the inner path cannot be efficiently rescanned
- Sort costs are included in the overall path cost when outersortkeys or innersortkeys are non-NIL
- Non-mergeable join conditions must be evaluated as qpquals during execution
- The algorithm performs best with high-cardinality, well-distributed join keys
- Memory usage is typically lower than hash joins since it does not require loading one side entirely into memory