# ExecInsertIndexTuples

## Location
[src/backend/executor/execIndexing.c:298-526](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execIndexing.c#L298-L526)

## Overview
Inserts index tuples into all indices associated with a result relation when a heap tuple is inserted, handling unique constraints, exclusion constraints, and various optimization scenarios including HOT updates and speculative insertions.

## Definition
```c
List *ExecInsertIndexTuples(ResultRelInfo *resultRelInfo,
                           TupleTableSlot *slot,
                           EState *estate,
                           bool update,
                           bool noDupErr,
                           bool *specConflict,
                           List *arbiterIndexes,
                           bool onlySummarizing)
```

## Detailed Description
ExecInsertIndexTuples is a comprehensive function that handles the insertion of index tuples across all indices associated with a relation. It is a central component of PostgreSQL tuple modification operations, managing both regular insertions and complex scenarios involving constraint checking, optimization hints, and conflict resolution.

The function performs several key operations:
1. Iterates through all indices associated with the result relation
2. Skips indices that are not ready for inserts or do not match the operation mode (summarizing vs. all)
3. Evaluates partial index predicates to determine if tuples should be indexed
4. Forms index datums from the heap tuple data using FormIndexDatum
5. Performs index insertions with appropriate uniqueness checking modes
6. Handles exclusion constraint checking for indices with exclusion operators
7. Collects information about potential constraint violations for deferred checking

The function supports multiple operation modes:
- Regular insertions with full constraint enforcement
- UPDATE operations with index-unchanged optimization hints
- Speculative insertions for INSERT ... ON CONFLICT operations
- Summarizing-only updates when HOT-like optimizations are applied
- Deferred constraint checking for deferrable unique constraints

## Parameters / Member Variables
- `resultRelInfo`: ResultRelInfo containing opened index relations and metadata
- `slot`: TupleTableSlot containing the heap tuple data to be indexed
- `estate`: Executor state containing expression evaluation context and other execution state
- `update`: Boolean indicating if this is part of an UPDATE operation (enables index-unchanged optimization)
- `noDupErr`: Boolean indicating whether to suppress duplicate key errors (used for speculative insertions)
- `specConflict`: Output parameter set to true if a speculative conflict is detected
- `arbiterIndexes`: List of index OIDs that should be considered for noDupErr behavior (NIL means all indices)
- `onlySummarizing`: Boolean indicating whether to update only summarizing indices (HOT-like optimization)

## Dependencies
- Functions called/Symbols referenced:
  - [ItemPointerIsValid](../I/ItemPointerIsValid.md): Validates the tuple ID pointer
  - GetPerTupleExprContext: Gets expression evaluation context for the current tuple
  - [ExecPrepareQual](ExecPrepareQual.md): Prepares partial index predicate expressions for evaluation
  - [ExecQual](ExecQual.md): Evaluates partial index predicates
  - [FormIndexDatum](../F/FormIndexDatum.md): Extracts index column values from the heap tuple
  - [list_member_oid](../l/list_member_oid.md): Checks if an index OID is in the arbiter list
  - [index_unchanged_by_update](../i/index_unchanged_by_update.md): Determines if an index was logically unchanged by an update
  - [index_insert](../i/index_insert.md): Performs the actual index tuple insertion with uniqueness checking
  - [check_exclusion_or_unique_constraint](../c/check_exclusion_or_unique_constraint.md): Checks exclusion constraints
  - [lappend_oid](../l/lappend_oid.md): Adds index OIDs to the result list for deferred checking
- Called from (representative examples):
  - [ExecInsert](ExecInsert.md): Main insertion path in nodeModifyTable
  - [ExecUpdateEpilogue](ExecUpdateEpilogue.md): After heap tuple updates in nodeModifyTable
  - [CopyFrom](../C/CopyFrom.md): Bulk data loading operations
  - [ExecSimpleRelationInsert](ExecSimpleRelationInsert.md)/Update: Logical replication operations

## Notes and Other Information
- Returns a list of index OIDs for any unique or exclusion constraints that had potential conflicts and require deferred checking
- Supports partial indices by evaluating predicate expressions before insertion
- Handles both immediate and deferred constraint checking modes
- The indexUnchanged optimization hint can significantly improve UPDATE performance when indices are logically unchanged
- Speculative insertion mode is used for INSERT ... ON CONFLICT to detect conflicts before committing
- Exclusion constraints are always checked after insertion, unlike unique constraints which can prevent insertion
- The function manages expression evaluation contexts to ensure proper memory management during index expression evaluation
- Summarizing indices are a special category that can be updated independently during certain optimization scenarios