# ModifyTableState

## Location
[src/include/nodes/execnodes.h:1355-1418](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L1355-L1418)

## Overview
ModifyTableState is a comprehensive execution state structure for ModifyTable nodes in PostgreSQL's executor, which handle data modification operations including INSERT, UPDATE, DELETE, and MERGE commands across single and partitioned tables.

## Definition
```c
typedef struct ModifyTableState
{
    PlanState         ps;                               /* its first field is NodeTag */
    CmdType           operation;                        /* INSERT, UPDATE, DELETE, or MERGE */
    bool              canSetTag;                        /* do we set the command tag/es_processed? */
    bool              mt_done;                          /* are we done? */
    int               mt_nrels;                         /* number of entries in resultRelInfo[] */
    ResultRelInfo    *resultRelInfo;                    /* info about target relation(s) */
    ResultRelInfo    *rootResultRelInfo;                /* root relation for tuple routing */
    EPQState          mt_epqstate;                      /* for evaluating EvalPlanQual rechecks */
    bool              fireBSTriggers;                   /* do we need to fire stmt triggers? */
    int               mt_resultOidAttno;                /* resno of "tableoid" junk attr */
    Oid               mt_lastResultOid;                 /* last-seen value of tableoid */
    int               mt_lastResultIndex;               /* corresponding index in resultRelInfo[] */
    HTAB             *mt_resultOidHash;                 /* optional hash table to speed lookups */
    TupleTableSlot   *mt_root_tuple_slot;               /* slot for root partition tuple */
    struct PartitionTupleRouting *mt_partition_tuple_routing;  /* tuple routing support */
    struct TransitionCaptureState *mt_transition_capture;      /* transition table control */
    struct TransitionCaptureState *mt_oc_transition_capture;   /* ON CONFLICT transition control */
    int               mt_merge_subcommands;             /* MERGE subcommand flags */
    MergeActionState *mt_merge_action;                  /* current MERGE action */
    TupleTableSlot   *mt_merge_pending_not_matched;     /* pending MERGE tuple */
    double            mt_merge_inserted;                /* MERGE insert counter */
    double            mt_merge_updated;                 /* MERGE update counter */
    double            mt_merge_deleted;                 /* MERGE delete counter */
} ModifyTableState;
```

## Detailed Description
ModifyTableState is one of the most complex executor state structures, managing all aspects of data modification operations in PostgreSQL. It handles single-table and multi-table operations, inheritance hierarchies, partitioned tables, and the sophisticated MERGE command. The structure maintains detailed state for concurrent update detection via EPQ (EvalPlanQual), trigger firing, tuple routing for partitioned tables, and transition table population for statement-level triggers. For MERGE operations, it tracks multiple operation types and maintains counters for each action performed.

## Parameters / Member Variables
- `ps`: Base PlanState structure containing common execution state fields
- `operation`: Type of modification operation (INSERT, UPDATE, DELETE, or MERGE)
- `canSetTag`: Boolean indicating whether this node should set the command tag and update es_processed
- `mt_done`: Boolean flag indicating whether the modification operation is complete
- `mt_nrels`: Number of target relations in the resultRelInfo array
- `resultRelInfo`: Array of ResultRelInfo structures containing information about target relations
- `rootResultRelInfo`: Information about the root relation mentioned in the original statement, used for statement triggers and tuple routing
- `mt_epqstate`: EPQState structure for handling concurrent update detection and EvalPlanQual rechecks
- `fireBSTriggers`: Boolean indicating whether before/after statement triggers need to be fired
- `mt_resultOidAttno`: Attribute number of the "tableoid" junk attribute for inherited operations
- `mt_lastResultOid`: Last observed tableoid value for optimization of inherited table lookups
- `mt_lastResultIndex`: Cached index in resultRelInfo array corresponding to mt_lastResultOid
- `mt_resultOidHash`: Optional hash table for fast OID-to-index lookups when many target relations exist
- `mt_root_tuple_slot`: Tuple slot for storing tuples in the root partitioned table's rowtype during UPDATEs
- `mt_partition_tuple_routing`: Pointer to partition tuple routing support structure
- `mt_transition_capture`: State for populating transition tables for the main operation
- `mt_oc_transition_capture`: State for populating transition tables for INSERT...ON CONFLICT UPDATE
- `mt_merge_subcommands`: Bitmask indicating which MERGE subcommands (INSERT/UPDATE/DELETE/DO NOTHING) are present
- `mt_merge_action`: Pointer to the currently executing MERGE action state
- `mt_merge_pending_not_matched`: Tuple slot holding a pending NOT MATCHED tuple for MERGE operations
- `mt_merge_inserted`: Counter for tuples inserted during MERGE operation
- `mt_merge_updated`: Counter for tuples updated during MERGE operation
- `mt_merge_deleted`: Counter for tuples deleted during MERGE operation

## Dependencies
- Functions called/Symbols referenced:
  - [PlanState](../P/PlanState.md) (inherited base structure)
  - CmdType (operation type enumeration)
  - [ResultRelInfo](../R/ResultRelInfo.md) (target relation information)
  - [EPQState](../E/EPQState.md) (concurrent update handling)
  - [HTAB](../H/HTAB.md) (hash table for OID lookups)
  - [PartitionTupleRouting](../P/PartitionTupleRouting.md) (partitioning support)
  - [TransitionCaptureState](../T/TransitionCaptureState.md) (transition table management)
  - [MergeActionState](MergeActionState.md) (MERGE operation state)
  - [TupleTableSlot](../T/TupleTableSlot.md) (tuple storage)
- Called from (representative examples):
  - [ExecModifyTable](../E/ExecModifyTable.md)
  - [ExecInitModifyTable](../E/ExecInitModifyTable.md)
  - [ExecEndModifyTable](../E/ExecEndModifyTable.md)
  - [ExecReScanModifyTable](../E/ExecReScanModifyTable.md)
  - [ExecInsert](../E/ExecInsert.md)
  - [ExecBatchInsert](../E/ExecBatchInsert.md)
  - [ExecOnConflictUpdate](../E/ExecOnConflictUpdate.md)
  - [ExecMergeMatched](../E/ExecMergeMatched.md)
  - [ExecMergeNotMatched](../E/ExecMergeNotMatched.md)

## Notes and Other Information
ModifyTableState represents the pinnacle of complexity in PostgreSQL's executor state structures, reflecting the sophisticated requirements of modern data modification operations. The structure's extensive partition and inheritance support enables PostgreSQL's advanced table organization features. The EPQ mechanism provides crucial MVCC support for concurrent operations. The MERGE-specific fields demonstrate PostgreSQL's implementation of SQL standard MERGE operations, allowing complex conditional data modification in a single statement. The transition capture mechanisms support advanced trigger functionality, enabling applications to track changes through statement-level triggers. This state structure is central to PostgreSQL's data modification capabilities and is extensively used throughout the executor and related subsystems.