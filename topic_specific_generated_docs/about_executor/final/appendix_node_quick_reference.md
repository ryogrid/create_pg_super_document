# Appendix E: Node Type Quick Reference

**PostgreSQL 17.6 Executor Subsystem -- Complete Node Type Lookup Table**

43 distinct plan node types enumerated from `execProcnode.c` dispatch tables.

---

## Control Nodes

| NodeTag | Plan Struct | PlanState Struct | Source File | Init / Exec / End |
|---------|-------------|-----------------|-------------|-------------------|
| `T_Result` | `Result` | `ResultState` | `nodeResult.c` | `ExecInitResult` / `ExecResult` / `ExecEndResult` |
| `T_ProjectSet` | `ProjectSet` | `ProjectSetState` | `nodeProjectSet.c` | `ExecInitProjectSet` / `ExecProjectSet` / `ExecEndProjectSet` |
| `T_ModifyTable` | `ModifyTable` | `ModifyTableState` | `nodeModifyTable.c` | `ExecInitModifyTable` / `ExecModifyTable` / `ExecEndModifyTable` |
| `T_Append` | `Append` | `AppendState` | `nodeAppend.c` | `ExecInitAppend` / `ExecAppend` / `ExecEndAppend` |
| `T_MergeAppend` | `MergeAppend` | `MergeAppendState` | `nodeMergeAppend.c` | `ExecInitMergeAppend` / `ExecMergeAppend` / `ExecEndMergeAppend` |
| `T_RecursiveUnion` | `RecursiveUnion` | `RecursiveUnionState` | `nodeRecursiveunion.c` | `ExecInitRecursiveUnion` / `ExecRecursiveUnion` / `ExecEndRecursiveUnion` |
| `T_BitmapAnd` | `BitmapAnd` | `BitmapAndState` | `nodeBitmapAnd.c` | `ExecInitBitmapAnd` / `MultiExecBitmapAnd` / `ExecEndBitmapAnd` |
| `T_BitmapOr` | `BitmapOr` | `BitmapOrState` | `nodeBitmapOr.c` | `ExecInitBitmapOr` / `MultiExecBitmapOr` / `ExecEndBitmapOr` |

## Scan Nodes

| NodeTag | Plan Struct | PlanState Struct | Source File | Init / Exec / End |
|---------|-------------|-----------------|-------------|-------------------|
| `T_SeqScan` | `SeqScan` | `SeqScanState` | `nodeSeqscan.c` | `ExecInitSeqScan` / `ExecSeqScan` / `ExecEndSeqScan` |
| `T_SampleScan` | `SampleScan` | `SampleScanState` | `nodeSamplescan.c` | `ExecInitSampleScan` / `ExecSampleScan` / `ExecEndSampleScan` |
| `T_IndexScan` | `IndexScan` | `IndexScanState` | `nodeIndexscan.c` | `ExecInitIndexScan` / `ExecIndexScan` / `ExecEndIndexScan` |
| `T_IndexOnlyScan` | `IndexOnlyScan` | `IndexOnlyScanState` | `nodeIndexonlyscan.c` | `ExecInitIndexOnlyScan` / `ExecIndexOnlyScan` / `ExecEndIndexOnlyScan` |
| `T_BitmapIndexScan` | `BitmapIndexScan` | `BitmapIndexScanState` | `nodeBitmapIndexscan.c` | `ExecInitBitmapIndexScan` / `MultiExecBitmapIndexScan` / `ExecEndBitmapIndexScan` |
| `T_BitmapHeapScan` | `BitmapHeapScan` | `BitmapHeapScanState` | `nodeBitmapHeapscan.c` | `ExecInitBitmapHeapScan` / `ExecBitmapHeapScan` / `ExecEndBitmapHeapScan` |
| `T_TidScan` | `TidScan` | `TidScanState` | `nodeTidscan.c` | `ExecInitTidScan` / `ExecTidScan` / `ExecEndTidScan` |
| `T_TidRangeScan` | `TidRangeScan` | `TidRangeScanState` | `nodeTidrangescan.c` | `ExecInitTidRangeScan` / `ExecTidRangeScan` / `ExecEndTidRangeScan` |
| `T_SubqueryScan` | `SubqueryScan` | `SubqueryScanState` | `nodeSubqueryscan.c` | `ExecInitSubqueryScan` / `ExecSubqueryScan` / `ExecEndSubqueryScan` |
| `T_FunctionScan` | `FunctionScan` | `FunctionScanState` | `nodeFunctionscan.c` | `ExecInitFunctionScan` / `ExecFunctionScan` / `ExecEndFunctionScan` |
| `T_TableFuncScan` | `TableFuncScan` | `TableFuncScanState` | `nodeTableFuncscan.c` | `ExecInitTableFuncScan` / `ExecTableFuncScan` / `ExecEndTableFuncScan` |
| `T_ValuesScan` | `ValuesScan` | `ValuesScanState` | `nodeValuesscan.c` | `ExecInitValuesScan` / `ExecValuesScan` / *(empty cleanup)* |
| `T_CteScan` | `CteScan` | `CteScanState` | `nodeCtescan.c` | `ExecInitCteScan` / `ExecCteScan` / `ExecEndCteScan` |
| `T_NamedTuplestoreScan` | `NamedTuplestoreScan` | `NamedTuplestoreScanState` | `nodeNamedtuplestorescan.c` | `ExecInitNamedTuplestoreScan` / `ExecNamedTuplestoreScan` / *(empty cleanup)* |
| `T_WorkTableScan` | `WorkTableScan` | `WorkTableScanState` | `nodeWorktablescan.c` | `ExecInitWorkTableScan` / `ExecWorkTableScan` / *(empty cleanup)* |
| `T_ForeignScan` | `ForeignScan` | `ForeignScanState` | `nodeForeignscan.c` | `ExecInitForeignScan` / `ExecForeignScan` / `ExecEndForeignScan` |
| `T_CustomScan` | `CustomScan` | `CustomScanState` | `nodeCustom.c` | `ExecInitCustomScan` / `ExecCustomScan` / `ExecEndCustomScan` |

## Join Nodes

| NodeTag | Plan Struct | PlanState Struct | Source File | Init / Exec / End |
|---------|-------------|-----------------|-------------|-------------------|
| `T_NestLoop` | `NestLoop` | `NestLoopState` | `nodeNestloop.c` | `ExecInitNestLoop` / `ExecNestLoop` / `ExecEndNestLoop` |
| `T_MergeJoin` | `MergeJoin` | `MergeJoinState` | `nodeMergejoin.c` | `ExecInitMergeJoin` / `ExecMergeJoin` / `ExecEndMergeJoin` |
| `T_HashJoin` | `HashJoin` | `HashJoinState` | `nodeHashjoin.c` | `ExecInitHashJoin` / `ExecHashJoin` (via `ExecHashJoinImpl`) / `ExecEndHashJoin` |

## Materialization / Sort Nodes

| NodeTag | Plan Struct | PlanState Struct | Source File | Init / Exec / End |
|---------|-------------|-----------------|-------------|-------------------|
| `T_Material` | `Material` | `MaterialState` | `nodeMaterial.c` | `ExecInitMaterial` / `ExecMaterial` / `ExecEndMaterial` |
| `T_Sort` | `Sort` | `SortState` | `nodeSort.c` | `ExecInitSort` / `ExecSort` / `ExecEndSort` |
| `T_IncrementalSort` | `IncrementalSort` | `IncrementalSortState` | `nodeIncrementalSort.c` | `ExecInitIncrementalSort` / `ExecIncrementalSort` / `ExecEndIncrementalSort` |
| `T_Memoize` | `Memoize` | `MemoizeState` | `nodeMemoize.c` | `ExecInitMemoize` / `ExecMemoize` / `ExecEndMemoize` |

## Aggregation / Grouping Nodes

| NodeTag | Plan Struct | PlanState Struct | Source File | Init / Exec / End |
|---------|-------------|-----------------|-------------|-------------------|
| `T_Group` | `Group` | `GroupState` | `nodeGroup.c` | `ExecInitGroup` / `ExecGroup` / `ExecEndGroup` |
| `T_Agg` | `Agg` | `AggState` | `nodeAgg.c` | `ExecInitAgg` / `ExecAgg` / `ExecEndAgg` |
| `T_WindowAgg` | `WindowAgg` | `WindowAggState` | `nodeWindowAgg.c` | `ExecInitWindowAgg` / `ExecWindowAgg` / `ExecEndWindowAgg` |
| `T_Unique` | `Unique` | `UniqueState` | `nodeUnique.c` | `ExecInitUnique` / `ExecUnique` / `ExecEndUnique` |
| `T_SetOp` | `SetOp` | `SetOpState` | `nodeSetOp.c` | `ExecInitSetOp` / `ExecSetOp` / `ExecEndSetOp` |

## Parallel Execution Nodes

| NodeTag | Plan Struct | PlanState Struct | Source File | Init / Exec / End |
|---------|-------------|-----------------|-------------|-------------------|
| `T_Gather` | `Gather` | `GatherState` | `nodeGather.c` | `ExecInitGather` / `ExecGather` / `ExecEndGather` |
| `T_GatherMerge` | `GatherMerge` | `GatherMergeState` | `nodeGatherMerge.c` | `ExecInitGatherMerge` / `ExecGatherMerge` / `ExecEndGatherMerge` |

## Data Modification / Locking Nodes

| NodeTag | Plan Struct | PlanState Struct | Source File | Init / Exec / End |
|---------|-------------|-----------------|-------------|-------------------|
| `T_LockRows` | `LockRows` | `LockRowsState` | `nodeLockRows.c` | `ExecInitLockRows` / `ExecLockRows` / `ExecEndLockRows` |

## Auxiliary Nodes

| NodeTag | Plan Struct | PlanState Struct | Source File | Init / Exec / End |
|---------|-------------|-----------------|-------------|-------------------|
| `T_Hash` | `Hash` | `HashState` | `nodeHash.c` | `ExecInitHash` / `MultiExecHash` / `ExecEndHash` |
| `T_Limit` | `Limit` | `LimitState` | `nodeLimit.c` | `ExecInitLimit` / `ExecLimit` / `ExecEndLimit` |

## SubPlan (Expression-Level, Not a Plan Node)

| NodeTag | Plan Struct | PlanState Struct | Source File | Init / Exec / End |
|---------|-------------|-----------------|-------------|-------------------|
| `T_SubPlan` | `SubPlan` | `SubPlanState` | `nodeSubplan.c` | `ExecInitSubPlan` / `ExecSubPlan` / `ExecEndSubPlan` |

---

## Notes

**Total node types**: 43 distinct plan node types.

**MultiExecProcNode nodes** (return non-tuple data structures):
- `T_Hash` returns a hash table via `MultiExecHash`
- `T_BitmapIndexScan` returns a `TIDBitmap` via `MultiExecBitmapIndexScan`
- `T_BitmapAnd` returns a `TIDBitmap` via `MultiExecBitmapAnd`
- `T_BitmapOr` returns a `TIDBitmap` via `MultiExecBitmapOr`

**Nodes with empty ExecEnd** (cleanup handled by memory context destruction):
- `T_ValuesScan`
- `T_NamedTuplestoreScan`
- `T_WorkTableScan`

**All scan nodes delegate to ExecScan()** with node-specific access/recheck methods:
SeqScan, SampleScan, IndexScan, IndexOnlyScan, BitmapHeapScan, TidScan,
TidRangeScan, SubqueryScan, FunctionScan, TableFuncScan, ValuesScan,
CteScan, NamedTuplestoreScan, WorkTableScan, ForeignScan, CustomScan.

**Parallel-aware node types**: SeqScan, IndexScan, IndexOnlyScan, BitmapHeapScan,
HashJoin, Hash, Append, MergeAppend.
