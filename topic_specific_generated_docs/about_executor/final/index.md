# PostgreSQL Executor Subsystem -- Documentation Index

**Version**: PostgreSQL 17.6
**Scope**: `src/backend/executor/`, `src/include/executor/`, `src/include/nodes/execnodes.h`
**Estimated total reading time**: 3.5 hours (all chapters)

---

## How to Use This Documentation

This documentation covers the PostgreSQL executor from architectural overview
down to individual function-level detail. It is organized into four tiers:

1. **Start here** if you are new to the executor: read Chapters 01-04 for a
   conceptual foundation (about 45 minutes).
2. **Core infrastructure** (Chapters 05-09) covers the internal machinery that
   all plan nodes depend on.
3. **Plan node catalog** (Chapters 10-19) provides detailed coverage of each
   node category.
4. **Appendices** serve as quick-reference material for daily development work.

All cross-references use the format `[Chapter NN](NN_filename.md)` for
navigation between sections.

---

## Part I -- Foundations

| # | Chapter | Key Symbols | Est. Time |
|---|---------|-------------|-----------|
| [01](01_executive_summary.md) | Executive Summary | -- | 5 min |
| [02](02_architecture_overview.md) | Architecture Overview | ExecutorStart, ExecutorRun, ExecProcNode, ExecInitNode | 15 min |
| [03](03_executor_lifecycle.md) | Executor Lifecycle | ExecutorStart, ExecutorRun, ExecutorFinish, ExecutorEnd, QueryDesc, EState, InitPlan, ExecutePlan | 25 min |
| [04](04_volcano_iterator_model.md) | Volcano Iterator Model | ExecProcNode, ExecInitNode, ExecEndNode, ExecReScan, PlanState, MultiExecProcNode | 20 min |

## Part II -- Core Infrastructure

| # | Chapter | Key Symbols | Est. Time |
|---|---------|-------------|-----------|
| [05](05_tuple_table_slot.md) | TupleTableSlot Abstraction | TupleTableSlot, TupleTableSlotOps, ExecClearTuple, ExecMaterializeSlot, slot_getsomeattrs | 20 min |
| [06](06_expression_evaluation.md) | Expression Evaluation | ExprState, ExecInitExpr, ExecInitExprRec, ExecInterpExpr, ExecQual, ExecProject, ExecEvalExpr, ProjectionInfo | 25 min |
| [07](07_memory_context_management.md) | Memory Context Management | EState, ExprContext, CreateExecutorState, FreeExecutorState, CreateExprContext, ResetExprContext | 15 min |
| 08 | Scan Node Infrastructure | ExecScan, ScanState, ExecScanAccessMtd, ExecScanRecheckMtd | 20 min |
| 09 | Plan-Executor Interface | PlannedStmt, Param, NestLoopParam, SubPlan, InitPlan | 15 min |

## Part III -- Plan Node Catalog

| # | Chapter | Key Symbols | Est. Time |
|---|---------|-------------|-----------|
| 10 | Join Nodes | ExecNestLoop, ExecMergeJoin, ExecHashJoinImpl, ExecHashTableCreate | 25 min |
| 11 | Aggregation and Grouping | ExecAgg, ExecInitAgg, ExecWindowAgg, AggState | 25 min |
| 12 | ModifyTable and DML | ExecModifyTable, ExecInsert, ExecUpdate, ExecDelete, ExecMerge | 25 min |
| 13 | Parallel Query Execution | ExecGather, ExecGatherMerge, ParallelQueryMain, ExecParallelInitializeDSM | 20 min |
| 14 | SPI (Server Programming Interface) | SPI_connect, SPI_execute, SPI_prepare, SPI_execute_plan | 15 min |
| 15 | Scan Nodes: Sequential and Sample | ExecSeqScan, ExecSampleScan | 10 min |
| 16 | Scan Nodes: Index and Bitmap | ExecIndexScan, ExecIndexOnlyScan, ExecBitmapHeapScan, MultiExecBitmapIndexScan | 15 min |
| 17 | Scan Nodes: Subquery, Function, and Virtual | ExecSubqueryScan, ExecFunctionScan, ExecValuesScan, ExecCteScan | 10 min |
| 18 | Materialization Nodes | ExecSort, ExecMaterial, ExecMemoize, ExecIncrementalSort | 15 min |
| 19 | Control and Utility Nodes | ExecResult, ExecAppend, ExecMergeAppend, ExecLimit, ExecRecursiveUnion | 15 min |

## Part IV -- Deep Dives

| # | Chapter | Topics | Est. Time |
|---|---------|--------|-----------|
| 20 | EXPLAIN ANALYZE Internals | Instrumentation, InstrAlloc, InstrStartNode, InstrStopNode, ExecProcNodeInstr | 10 min |

## Appendices

| # | Appendix | Description |
|---|----------|-------------|
| A | [Symbol Index](appendix_symbol_index.md) | Alphabetical index of all documented symbols with chapter links |
| B | [Glossary](appendix_glossary.md) | Terminology definitions (plan node, qual, slot, deforming, etc.) |
| C | [Data Structures](appendix_data_structures.md) | Complete struct definitions: QueryDesc, EState, PlanState, ExprState, ExprContext, TupleTableSlot |
| D | [Node Quick Reference](appendix_node_quick_reference.md) | One-line summary of all 43 plan node types with source file locations |

## Reference Cards

| Document | Description |
|----------|-------------|
| [Quick Reference](reference_quick_reference.md) | Two-page summary: critical paths, key functions, memory context rules |
| [API Reference](reference_api_reference.md) | Function signature catalog grouped by subsystem |
| [Quality Report](reference_quality_report.md) | Coverage metrics, verified symbols, diagram inventory |

---

## Source File Map

| File | Size | Chapter Coverage |
|------|------|-----------------|
| `execMain.c` | 92 KB | [03](03_executor_lifecycle.md) |
| `execProcnode.c` | 27 KB | [04](04_volcano_iterator_model.md) |
| `execScan.c` | 9 KB | 08 |
| `execExpr.c` | 138 KB | [06](06_expression_evaluation.md) |
| `execExprInterp.c` | 148 KB | [06](06_expression_evaluation.md) |
| `execTuples.c` | 66 KB | [05](05_tuple_table_slot.md) |
| `execUtils.c` | 39 KB | [07](07_memory_context_management.md) |
| `execAmi.c` | 17 KB | [04](04_volcano_iterator_model.md) |
| `execParallel.c` | 48 KB | 13 |
| `execPartition.c` | 80 KB | 12 |
| `nodeModifyTable.c` | 161 KB | 12 |
| `nodeAgg.c` | 150 KB | 11 |
| `nodeWindowAgg.c` | 116 KB | 11 |
| `nodeHash.c` | 112 KB | 10 |
| `nodeHashjoin.c` | 53 KB | 10 |
| `nodeMergejoin.c` | 50 KB | 10 |
| `nodeIndexscan.c` | 52 KB | 16 |
| `spi.c` | 88 KB | 14 |
| `instrument.c` | 9 KB | 20 |

---

## Critical Execution Paths

For quick orientation, these are the ten most important call chains through
the executor. Each path links to the chapter where it is documented in detail.

1. **Startup**: `PortalRunSelect` -> `ExecutorStart` -> `CreateExecutorState` -> `InitPlan` -> `ExecInitNode`
   See [Chapter 03](03_executor_lifecycle.md)

2. **Tuple fetch**: `ExecutePlan` -> `ExecProcNode` -> `ExecSeqScan` -> `ExecScan` -> `ExecQual` -> `ExecProject`
   See [Chapters 04](04_volcano_iterator_model.md), [06](06_expression_evaluation.md), 08

3. **Expression compilation**: `ExecInitExpr` -> `ExecInitExprRec` -> `ExecReadyExpr`
   See [Chapter 06](06_expression_evaluation.md)

4. **Expression evaluation**: `ExecEvalExpr` -> `ExecInterpExpr` (step loop)
   See [Chapter 06](06_expression_evaluation.md)

5. **Memory lifecycle**: `CreateExecutorState` -> `CreateExprContext` -> `ResetExprContext` -> `FreeExecutorState`
   See [Chapter 07](07_memory_context_management.md)

6. **Hash join**: `ExecHashJoinImpl` -> `MultiExecHash` -> `ExecHashTableCreate` -> `ExecScanHashBucket`
   See Chapter 10

7. **Aggregation**: `ExecAgg` -> `agg_fill_hash_table` -> `ExecProcNode` -> `agg_retrieve_hash_table`
   See Chapter 11

8. **DML**: `ExecModifyTable` -> `ExecPrepareTupleRouting` -> `ExecInsert` -> `ExecInsertIndexTuples`
   See Chapter 12

9. **Parallel gather**: `ExecGather` -> `ExecParallelInitializeDSM` -> `ParallelQueryMain`
   See Chapter 13

10. **Shutdown**: `ExecutorFinish` -> `ExecutorEnd` -> `ExecEndPlan` -> `ExecEndNode` -> `FreeExecutorState`
    See [Chapter 03](03_executor_lifecycle.md)

---

## Diagrams

All diagrams are in Mermaid format and can be rendered with any Mermaid-compatible viewer.

| Diagram | Type | Relevant Chapter |
|---------|------|-----------------|
| `executor_lifecycle.mermaid` | State diagram | [03](03_executor_lifecycle.md) |
| `volcano_tuple_flow.mermaid` | Sequence diagram | [04](04_volcano_iterator_model.md) |
| `node_dispatch_flowchart.mermaid` | Flowchart | [04](04_volcano_iterator_model.md) |
| `node_type_taxonomy.mermaid` | Class diagram | Appendix D |
| `tuple_slot_hierarchy.mermaid` | Class diagram | [05](05_tuple_table_slot.md) |
| `expression_pipeline.mermaid` | Flowchart | [06](06_expression_evaluation.md) |
| `hashjoin_two_phase.mermaid` | Sequence diagram | Chapter 10 |
| `mergejoin_state_machine.mermaid` | State diagram | Chapter 10 |
| `modifytable_dispatch.mermaid` | Flowchart | Chapter 12 |
| `parallel_query_architecture.mermaid` | Architecture diagram | Chapter 13 |
