# HashJoinState

## Location
[src/include/nodes/execnodes.h:2189-2209](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L2189-L2209)

## Overview
HashJoinState is the execution state structure for hash join operations in PostgreSQL, containing all runtime information needed to perform hash join execution.

## Definition
```c
typedef struct HashJoinState
{
    JoinState   js;                     /* its first field is NodeTag */
    ExprState  *hashclauses;
    List       *hj_OuterHashKeys;       /* list of ExprState nodes */
    List       *hj_HashOperators;       /* list of operator OIDs */
    List       *hj_Collations;
    HashJoinTable hj_HashTable;
    uint32      hj_CurHashValue;
    int         hj_CurBucketNo;
    int         hj_CurSkewBucketNo;
    HashJoinTuple hj_CurTuple;
    TupleTableSlot *hj_OuterTupleSlot;
    TupleTableSlot *hj_HashTupleSlot;
    TupleTableSlot *hj_NullOuterTupleSlot;
    TupleTableSlot *hj_NullInnerTupleSlot;
    TupleTableSlot *hj_FirstOuterTupleSlot;
    int         hj_JoinState;
    bool        hj_MatchedOuter;
    bool        hj_OuterNotEmpty;
} HashJoinState;
```

## Detailed Description
HashJoinState represents the complete execution state for a hash join node in PostgreSQLs execution tree. It extends the base JoinState structure and contains all the specific state information needed for hash join operations. The structure tracks the current position in hash table scanning, manages tuple slots for different join scenarios, and maintains the hash table itself. It supports various join types including inner joins, left/right outer joins, and full outer joins by tracking match status and providing specialized tuple slots for handling NULL values.

The state machine progresses through different phases: building the hash table from the inner relation, probing with tuples from the outer relation, and potentially handling unmatched tuples for outer joins. The structure also supports batch processing for large datasets and parallel execution across multiple worker processes.

## Parameters / Member Variables
- `js`: Base JoinState structure containing common join execution state
- `hashclauses`: Compiled expressions for hash join conditions
- `hj_OuterHashKeys`: List of ExprState nodes for outer relation hash keys
- `hj_HashOperators`: List of operator OIDs used for hash computations
- `hj_Collations`: Collation specifications for hash operations
- `hj_HashTable`: The actual hash table structure containing inner relation data
- `hj_CurHashValue`: Current hash value being processed during scanning
- `hj_CurBucketNo`: Current bucket number in the hash table being scanned
- `hj_CurSkewBucketNo`: Current skew bucket number (for skew optimization)
- `hj_CurTuple`: Current tuple being examined from the hash table
- `hj_OuterTupleSlot`: Tuple slot for outer relation tuples
- `hj_HashTupleSlot`: Tuple slot for hash table (inner) tuples
- `hj_NullOuterTupleSlot`: Tuple slot containing NULL values for outer join padding
- `hj_NullInnerTupleSlot`: Tuple slot containing NULL values for inner join padding  
- `hj_FirstOuterTupleSlot`: Slot for the first outer tuple in batch processing
- `hj_JoinState`: Current state of the hash join state machine
- `hj_MatchedOuter`: Flag indicating if current outer tuple found a match
- `hj_OuterNotEmpty`: Flag indicating if outer relation contains any tuples

## Dependencies
- Functions called/Symbols referenced:
  - [JoinState](../J/JoinState.md) (base join execution state)
  - [HashJoinTable](HashJoinTable.md) (hash table structure)
  - [HashJoinTuple](HashJoinTuple.md) (individual hash table entries)
  - ExprState (expression evaluation state)
  - TupleTableSlot (tuple storage and manipulation)
- Called from (representative examples):
  - [ExecHashJoinImpl](../E/ExecHashJoinImpl.md)
  - [ExecInitHashJoin](../E/ExecInitHashJoin.md)
  - [ExecEndHashJoin](../E/ExecEndHashJoin.md)
  - [ExecReScanHashJoin](../E/ExecReScanHashJoin.md)
  - [ExecHashJoinOuterGetTuple](../E/ExecHashJoinOuterGetTuple.md)

## Notes and Other Information
- The structure inherits from JoinState, making it compatible with generic join processing functions
- Hash join execution follows a state machine pattern with states like HJ_BUILD_HASHTABLE, HJ_NEED_NEW_OUTER, HJ_SCAN_BUCKET
- Supports both regular and parallel hash join execution through the same interface
- The various tuple slots enable efficient handling of different join types without unnecessary tuple copying
- Skew optimization uses separate tracking (hj_CurSkewBucketNo) to handle highly skewed data distributions
- Memory management is handled through the hash tables memory contexts, not directly by this structure