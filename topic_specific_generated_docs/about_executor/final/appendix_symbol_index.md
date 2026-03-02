# Appendix A: Symbol Index

**PostgreSQL 17.6 Executor Subsystem -- Alphabetical Symbol Reference**

This index lists all key executor symbols with their source file locations,
importance scores, and which documentation chapter covers them.

---

## Symbol Reference Table

| # | Symbol | Source File | Score | Category | Documented In |
|---|--------|-------------|-------|----------|---------------|
| 1 | `CreateExecutorState` | src/backend/executor/execMain.c | 0.85 | MEMORY_MANAGEMENT | Ch.3 Lifecycle, Appendix D |
| 2 | `CreateQueryDesc` | src/backend/executor/pquery.c | 0.78 | EXECUTOR_LIFECYCLE | Ch.3 Lifecycle |
| 3 | `EState` | src/include/nodes/execnodes.h | 0.95 | MEMORY_MANAGEMENT | Ch.3 Lifecycle, Appendix D |
| 4 | `EvalPlanQual` | src/backend/executor/execMain.c | 0.72 | DATA_MODIFICATION | Ch.8 ModifyTable |
| 5 | `ExecAgg` | src/backend/executor/nodeAgg.c | 0.90 | AGGREGATION | Ch.7 Aggregation, Deep Dive 2 |
| 6 | `ExecAppend` | src/backend/executor/nodeAppend.c | 0.78 | CONTROL_NODES | Ch.5 Control Nodes |
| 7 | `ExecBatchInsert` | src/backend/executor/nodeModifyTable.c | 0.62 | DATA_MODIFICATION | Ch.8 ModifyTable |
| 8 | `ExecBitmapHeapScan` | src/backend/executor/nodeBitmapHeapscan.c | 0.78 | SCAN_NODES | Ch.6 Scan Nodes |
| 9 | `ExecBuildProjectionInfo` | src/backend/executor/execExpr.c | 0.75 | EXPRESSION_EVAL | Ch.10 Expressions, API Ref |
| 10 | `ExecCrossPartitionUpdate` | src/backend/executor/nodeModifyTable.c | 0.68 | DATA_MODIFICATION | Ch.8 ModifyTable |
| 11 | `ExecDelete` | src/backend/executor/nodeModifyTable.c | 0.82 | DATA_MODIFICATION | Ch.8 ModifyTable |
| 12 | `ExecEndNode` | src/backend/executor/execProcnode.c | 0.90 | VOLCANO_DISPATCH | Ch.4 Volcano, API Ref |
| 13 | `ExecEndPlan` | src/backend/executor/execMain.c | 0.80 | EXECUTOR_LIFECYCLE | Ch.3 Lifecycle |
| 14 | `ExecEvalExpr` | src/include/executor/executor.h | 0.90 | EXPRESSION_EVAL | Ch.10 Expressions, API Ref |
| 15 | `ExecFindPartition` | src/backend/executor/execPartition.c | 0.75 | DATA_MODIFICATION | Ch.8 ModifyTable |
| 16 | `ExecGather` | src/backend/executor/nodeGather.c | 0.85 | PARALLEL_EXECUTION | Ch.9 Parallel, Deep Dive 4 |
| 17 | `ExecGatherMerge` | src/backend/executor/nodeGatherMerge.c | 0.78 | PARALLEL_EXECUTION | Ch.9 Parallel |
| 18 | `ExecGroup` | src/backend/executor/nodeGroup.c | 0.60 | AGGREGATION | Ch.7 Aggregation |
| 19 | `ExecHashJoinImpl` | src/backend/executor/nodeHashjoin.c | 0.92 | JOIN_NODES | Ch.11 Joins, Deep Dive 1 |
| 20 | `ExecHashJoinNewBatch` | src/backend/executor/nodeHashjoin.c | 0.68 | JOIN_NODES | Deep Dive 1 |
| 21 | `ExecHashTableCreate` | src/backend/executor/nodeHash.c | 0.78 | JOIN_NODES | Ch.11 Joins, Deep Dive 1 |
| 22 | `ExecIncrementalSort` | src/backend/executor/nodeIncrementalSort.c | 0.68 | MATERIALIZATION | Ch.12 Materialization |
| 23 | `ExecIndexOnlyScan` | src/backend/executor/nodeIndexonlyscan.c | 0.78 | SCAN_NODES | Ch.6 Scan Nodes |
| 24 | `ExecIndexScan` | src/backend/executor/nodeIndexscan.c | 0.85 | SCAN_NODES | Ch.6 Scan Nodes |
| 25 | `ExecInitAgg` | src/backend/executor/nodeAgg.c | 0.82 | AGGREGATION | Ch.7 Aggregation, Deep Dive 2 |
| 26 | `ExecInitAppend` | src/backend/executor/nodeAppend.c | 0.72 | CONTROL_NODES | Ch.5 Control Nodes |
| 27 | `ExecInitBitmapHeapScan` | src/backend/executor/nodeBitmapHeapscan.c | 0.70 | SCAN_NODES | Ch.6 Scan Nodes |
| 28 | `ExecInitBitmapIndexScan` | src/backend/executor/nodeBitmapIndexscan.c | 0.65 | SCAN_NODES | Ch.6 Scan Nodes |
| 29 | `ExecInitExpr` | src/backend/executor/execExpr.c | 0.92 | EXPRESSION_EVAL | Ch.10 Expressions, API Ref |
| 30 | `ExecInitExprRec` | src/backend/executor/execExpr.c | 0.88 | EXPRESSION_EVAL | Ch.10 Expressions |
| 31 | `ExecInitGather` | src/backend/executor/nodeGather.c | 0.75 | PARALLEL_EXECUTION | Ch.9 Parallel |
| 32 | `ExecInitHashJoin` | src/backend/executor/nodeHashjoin.c | 0.78 | JOIN_NODES | Ch.11 Joins |
| 33 | `ExecInitIndexOnlyScan` | src/backend/executor/nodeIndexonlyscan.c | 0.70 | SCAN_NODES | Ch.6 Scan Nodes |
| 34 | `ExecInitIndexScan` | src/backend/executor/nodeIndexscan.c | 0.78 | SCAN_NODES | Ch.6 Scan Nodes |
| 35 | `ExecInitMergeJoin` | src/backend/executor/nodeMergejoin.c | 0.75 | JOIN_NODES | Ch.11 Joins |
| 36 | `ExecInitModifyTable` | src/backend/executor/nodeModifyTable.c | 0.85 | DATA_MODIFICATION | Ch.8 ModifyTable |
| 37 | `ExecInitNode` | src/backend/executor/execProcnode.c | 0.99 | VOLCANO_DISPATCH | Ch.4 Volcano, API Ref |
| 38 | `ExecInitPartitionInfo` | src/backend/executor/execPartition.c | 0.68 | DATA_MODIFICATION | Ch.8 ModifyTable |
| 39 | `ExecInitPartitionPruning` | src/backend/executor/execPartition.c | 0.65 | DATA_MODIFICATION | Ch.8 ModifyTable |
| 40 | `ExecInitQual` | src/backend/executor/execExpr.c | 0.88 | EXPRESSION_EVAL | Ch.10 Expressions, API Ref |
| 41 | `ExecInitSeqScan` | src/backend/executor/nodeSeqscan.c | 0.80 | SCAN_NODES | Ch.6 Scan Nodes |
| 42 | `ExecInitSort` | src/backend/executor/nodeSort.c | 0.72 | MATERIALIZATION | Ch.12 Materialization |
| 43 | `ExecInitWindowAgg` | src/backend/executor/nodeWindowAgg.c | 0.72 | AGGREGATION | Ch.7 Aggregation |
| 44 | `ExecInsert` | src/backend/executor/nodeModifyTable.c | 0.88 | DATA_MODIFICATION | Ch.8 ModifyTable |
| 45 | `ExecInterpExpr` | src/backend/executor/execExprInterp.c | 0.88 | EXPRESSION_EVAL | Ch.10 Expressions, Deep Dive 5 |
| 46 | `ExecLimit` | src/backend/executor/nodeLimit.c | 0.72 | CONTROL_NODES | Ch.5 Control Nodes |
| 47 | `ExecLockRows` | src/backend/executor/nodeLockRows.c | 0.68 | DATA_MODIFICATION | Ch.8 ModifyTable |
| 48 | `ExecMaterial` | src/backend/executor/nodeMaterial.c | 0.68 | MATERIALIZATION | Ch.12 Materialization |
| 49 | `ExecMemoize` | src/backend/executor/nodeMemoize.c | 0.65 | MATERIALIZATION | Ch.12 Materialization |
| 50 | `ExecMerge` | src/backend/executor/nodeModifyTable.c | 0.78 | DATA_MODIFICATION | Ch.8 ModifyTable |
| 51 | `ExecMergeJoin` | src/backend/executor/nodeMergejoin.c | 0.88 | JOIN_NODES | Ch.11 Joins, Deep Dive 3 |
| 52 | `ExecModifyTable` | src/backend/executor/nodeModifyTable.c | 0.95 | DATA_MODIFICATION | Ch.8 ModifyTable |
| 53 | `ExecNestLoop` | src/backend/executor/nodeNestloop.c | 0.88 | JOIN_NODES | Ch.11 Joins |
| 54 | `ExecOnConflictUpdate` | src/backend/executor/nodeModifyTable.c | 0.72 | DATA_MODIFICATION | Ch.8 ModifyTable |
| 55 | `ExecParallelInitializeDSM` | src/backend/executor/execParallel.c | 0.75 | PARALLEL_EXECUTION | Ch.9 Parallel, Deep Dive 4 |
| 56 | `ExecPrepareTupleRouting` | src/backend/executor/nodeModifyTable.c | 0.72 | DATA_MODIFICATION | Ch.8 ModifyTable |
| 57 | `ExecProcNode` | src/include/executor/executor.h | 0.99 | VOLCANO_DISPATCH | Ch.4 Volcano, API Ref |
| 58 | `ExecProject` | src/include/executor/executor.h | 0.90 | EXPRESSION_EVAL | Ch.10 Expressions, API Ref |
| 59 | `ExecProjectSet` | src/backend/executor/nodeProjectSet.c | 0.65 | CONTROL_NODES | Ch.5 Control Nodes |
| 60 | `ExecQual` | src/include/executor/executor.h | 0.92 | EXPRESSION_EVAL | Ch.10 Expressions, API Ref |
| 61 | `ExecReScan` | src/backend/executor/execAmi.c | 0.88 | VOLCANO_DISPATCH | Ch.4 Volcano, API Ref |
| 62 | `ExecRecursiveUnion` | src/backend/executor/nodeRecursiveunion.c | 0.68 | CONTROL_NODES | Ch.5 Control Nodes |
| 63 | `ExecResult` | src/backend/executor/nodeResult.c | 0.75 | CONTROL_NODES | Ch.5 Control Nodes |
| 64 | `ExecScan` | src/backend/executor/execScan.c | 0.95 | SCAN_INFRASTRUCTURE | Ch.6 Scan Nodes, API Ref |
| 65 | `ExecScanHashBucket` | src/backend/executor/nodeHash.c | 0.72 | JOIN_NODES | Ch.11 Joins, Deep Dive 1 |
| 66 | `ExecSeqScan` | src/backend/executor/nodeSeqscan.c | 0.85 | SCAN_NODES | Ch.6 Scan Nodes |
| 67 | `ExecSetExecProcNode` | src/backend/executor/execProcnode.c | 0.75 | VOLCANO_DISPATCH | Ch.4 Volcano |
| 68 | `ExecSetOp` | src/backend/executor/nodeSetOp.c | 0.60 | AGGREGATION | Ch.7 Aggregation |
| 69 | `ExecSetTupleBound` | src/backend/executor/execProcnode.c | 0.55 | VOLCANO_DISPATCH | Ch.4 Volcano |
| 70 | `ExecShutdownNode` | src/backend/executor/execProcnode.c | 0.60 | VOLCANO_DISPATCH | Ch.4 Volcano |
| 71 | `ExecSort` | src/backend/executor/nodeSort.c | 0.82 | MATERIALIZATION | Ch.12 Materialization |
| 72 | `ExecUnique` | src/backend/executor/nodeUnique.c | 0.58 | AGGREGATION | Ch.7 Aggregation |
| 73 | `ExecUpdate` | src/backend/executor/nodeModifyTable.c | 0.85 | DATA_MODIFICATION | Ch.8 ModifyTable |
| 74 | `ExecWindowAgg` | src/backend/executor/nodeWindowAgg.c | 0.82 | AGGREGATION | Ch.7 Aggregation, Deep Dive 2 |
| 75 | `ExecutePlan` | src/backend/executor/execMain.c | 0.94 | EXECUTOR_LIFECYCLE | Ch.3 Lifecycle |
| 76 | `ExecutorEnd` | src/backend/executor/execMain.c | 0.90 | EXECUTOR_LIFECYCLE | Ch.3 Lifecycle, API Ref |
| 77 | `ExecutorFinish` | src/backend/executor/execMain.c | 0.90 | EXECUTOR_LIFECYCLE | Ch.3 Lifecycle, API Ref |
| 78 | `ExecutorRun` | src/backend/executor/execMain.c | 1.00 | EXECUTOR_LIFECYCLE | Ch.3 Lifecycle, API Ref |
| 79 | `ExecutorStart` | src/backend/executor/execMain.c | 1.00 | EXECUTOR_LIFECYCLE | Ch.3 Lifecycle, API Ref |
| 80 | `ExprContext` | src/include/nodes/execnodes.h | 0.88 | MEMORY_MANAGEMENT | Ch.10 Expressions, Appendix D |
| 81 | `ExprState` | src/include/nodes/execnodes.h | 0.85 | EXPRESSION_EVAL | Ch.10 Expressions, Appendix D |
| 82 | `InitPlan` | src/backend/executor/execMain.c | 0.95 | EXECUTOR_LIFECYCLE | Ch.3 Lifecycle |
| 83 | `InitResultRelInfo` | src/backend/executor/execMain.c | 0.65 | DATA_MODIFICATION | Ch.8 ModifyTable |
| 84 | `MultiExecBitmapIndexScan` | src/backend/executor/nodeBitmapIndexscan.c | 0.65 | SCAN_NODES | Ch.6 Scan Nodes |
| 85 | `MultiExecHash` | src/backend/executor/nodeHash.c | 0.82 | JOIN_NODES | Ch.11 Joins, Deep Dive 1 |
| 86 | `MultiExecProcNode` | src/backend/executor/execProcnode.c | 0.80 | VOLCANO_DISPATCH | Ch.4 Volcano, API Ref |
| 87 | `ParallelQueryMain` | src/backend/executor/execParallel.c | 0.78 | PARALLEL_EXECUTION | Ch.9 Parallel, Deep Dive 4 |
| 88 | `PlanState` | src/include/nodes/execnodes.h | 0.95 | VOLCANO_DISPATCH | Ch.4 Volcano, Appendix D |
| 89 | `PortalRunSelect` | src/backend/tcop/pquery.c | 0.82 | EXECUTOR_LIFECYCLE | Ch.3 Lifecycle |
| 90 | `ProjectionInfo` | src/include/nodes/execnodes.h | 0.75 | EXPRESSION_EVAL | Ch.10 Expressions |
| 91 | `QueryDesc` | src/include/executor/execdesc.h | 0.92 | EXECUTOR_LIFECYCLE | Ch.3 Lifecycle, Appendix D |
| 92 | `SPI_connect` | src/backend/executor/spi.c | 0.75 | SPI | API Ref |
| 93 | `SPI_execute` | src/backend/executor/spi.c | 0.75 | SPI | API Ref |
| 94 | `TupleTableSlot` | src/include/executor/tuptable.h | 0.95 | TUPLE_TABLE | Ch.13 Tuple Table, Appendix D |
| 95 | `TupleTableSlotOps` | src/include/executor/tuptable.h | 0.82 | TUPLE_TABLE | Ch.13 Tuple Table |
| 96 | `standard_ExecutorRun` | src/backend/executor/execMain.c | 0.97 | EXECUTOR_LIFECYCLE | Ch.3 Lifecycle |
| 97 | `standard_ExecutorStart` | src/backend/executor/execMain.c | 0.98 | EXECUTOR_LIFECYCLE | Ch.3 Lifecycle |

---

## Symbols by Category

### EXECUTOR_LIFECYCLE (14 symbols)
`ExecutorStart`, `standard_ExecutorStart`, `ExecutorRun`, `standard_ExecutorRun`,
`ExecutorFinish`, `ExecutorEnd`, `InitPlan`, `ExecutePlan`, `ExecEndPlan`,
`CreateExecutorState`, `CreateQueryDesc`, `QueryDesc`, `EState`, `PortalRunSelect`

### VOLCANO_DISPATCH (10 symbols)
`ExecInitNode`, `ExecProcNode`, `ExecEndNode`, `ExecReScan`, `ExecSetExecProcNode`,
`MultiExecProcNode`, `ExecShutdownNode`, `ExecSetTupleBound`, `PlanState`, `ExprContext`

### EXPRESSION_EVAL (10 symbols)
`ExecInitExpr`, `ExecInitExprRec`, `ExecInitQual`, `ExecEvalExpr`, `ExecQual`,
`ExecProject`, `ExecBuildProjectionInfo`, `ExecInterpExpr`, `ExprState`, `ProjectionInfo`

### SCAN_INFRASTRUCTURE / SCAN_NODES (12 symbols)
`ExecScan`, `ExecSeqScan`, `ExecInitSeqScan`, `ExecIndexScan`, `ExecInitIndexScan`,
`ExecIndexOnlyScan`, `ExecInitIndexOnlyScan`, `ExecBitmapHeapScan`,
`ExecInitBitmapHeapScan`, `ExecInitBitmapIndexScan`, `MultiExecBitmapIndexScan`

### JOIN_NODES (12 symbols)
`ExecNestLoop`, `ExecMergeJoin`, `ExecHashJoinImpl`, `ExecInitHashJoin`,
`ExecInitMergeJoin`, `ExecHashTableCreate`, `MultiExecHash`, `ExecScanHashBucket`,
`ExecHashJoinNewBatch`, `ExecInitHash`, `ExecInitNestLoop`

### DATA_MODIFICATION (16 symbols)
`ExecModifyTable`, `ExecInitModifyTable`, `ExecInsert`, `ExecUpdate`, `ExecDelete`,
`ExecMerge`, `ExecOnConflictUpdate`, `ExecCrossPartitionUpdate`, `ExecFindPartition`,
`ExecPrepareTupleRouting`, `ExecInitPartitionInfo`, `ExecInitPartitionPruning`,
`ExecBatchInsert`, `InitResultRelInfo`, `EvalPlanQual`, `ExecLockRows`

### AGGREGATION (8 symbols)
`ExecAgg`, `ExecInitAgg`, `ExecWindowAgg`, `ExecInitWindowAgg`, `ExecGroup`,
`ExecUnique`, `ExecSetOp`, `ExecSort`

### PARALLEL_EXECUTION (7 symbols)
`ExecGather`, `ExecInitGather`, `ExecGatherMerge`, `ExecInitGatherMerge`,
`ExecParallelInitializeDSM`, `ParallelQueryMain`

### CONTROL_NODES (6 symbols)
`ExecAppend`, `ExecInitAppend`, `ExecLimit`, `ExecRecursiveUnion`,
`ExecProjectSet`, `ExecResult`

### MATERIALIZATION (5 symbols)
`ExecSort`, `ExecInitSort`, `ExecIncrementalSort`, `ExecMaterial`, `ExecMemoize`

### TUPLE_TABLE (2 symbols)
`TupleTableSlot`, `TupleTableSlotOps`

### SPI (2 symbols)
`SPI_connect`, `SPI_execute`
