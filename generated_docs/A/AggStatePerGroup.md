# AggStatePerGroup

## Location
src/include/nodes/execnodes.h: 2459 - 2459

## Overview
AggStatePerGroup is a typedef pointer to AggStatePerGroupData structure that maintains the working state values for each aggregate function within a specific input group during aggregate computation.

## Definition
```c
typedef struct AggStatePerGroupData *AggStatePerGroup;
```

Where AggStatePerGroupData is defined as:
```c
typedef struct AggStatePerGroupData
{
    Datum       transValue;        /* current transition value */
    bool        transValueIsNull;
    bool        noTransValue;      /* true if transValue not set yet */
} AggStatePerGroupData;
```

## Detailed Description
AggStatePerGroup represents the per-aggregate-per-group working state that tracks the current transition value for each aggregate function within a specific input group. This structure is fundamental to PostgreSQL's aggregation system as it maintains the intermediate state that gets updated as each input tuple is processed. The structure varies in usage depending on the aggregation mode: in AGG_PLAIN and AGG_SORTED modes, there's a single array reused for each group, while in AGG_HASHED mode, each hash table entry contains its own array of these structures. The design efficiently handles NULL value semantics and the special case of uninitialized transition values.

## Parameters / Member Variables
- `transValue`: The current transition value for the aggregate function, updated as input tuples are processed
- `transValueIsNull`: Boolean flag indicating whether the current transition value is NULL
- `noTransValue`: Boolean flag indicating whether the transition value has been set yet (true if uninitialized)

## Dependencies
- Functions called/Symbols referenced:
  - AggStatePerGroupData
  - Datum (PostgreSQL's generic data value type)
- Called from (representative examples):
  - ExecInterpExpr (various aggregate evaluation functions)
  - ExecAggInitGroup
  - ExecAggPlainTransByVal
  - ExecAggPlainTransByRef
  - initialize_aggregate
  - advance_transition_function
  - process_ordered_aggregate_single
  - process_ordered_aggregate_multi
  - finalize_aggregate
  - finalize_partialaggregate
  - initialize_hash_entry
  - lookup_hash_entries
  - agg_retrieve_direct
  - agg_retrieve_hash_table_in_memory

## Notes and Other Information
- The structure uses field number defines (FIELDNO_AGGSTATEPERGROUPDATA_*) for efficient access in compiled expressions
- The noTransValue and transValueIsNull flags have subtle but important semantic differences: noTransValue tracks initialization state while transValueIsNull tracks actual NULL values returned by transition functions
- Initially, noTransValue and transValueIsNull have the same value, but they diverge once the first value is processed
- Only the first non-NULL input will be auto-substituted when noTransValue is true; subsequent NULL results from the transition function are preserved
- In AGG_HASHED mode, DISTINCT aggregates are not supported, which influences the structure's design and memory layout
- The sortstate field mentioned in comments is deliberately kept separate for space efficiency in hash table scenarios
- Critical for maintaining aggregate state consistency across different aggregation modes (plain, sorted, hashed)