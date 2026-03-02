# Chapter 01 -- Executive Summary

**Prerequisites**: None
**Next**: [Chapter 02 -- Architecture Overview](02_architecture_overview.md)

---

## What the Executor Does

The PostgreSQL executor is the component that takes a query plan produced by the
planner/optimizer and actually executes it, producing result tuples. It sits at
the end of the query processing pipeline:

    parse -> analyze -> rewrite -> plan -> **execute**

Every SQL statement -- whether a simple `SELECT`, a complex multi-way join, an
`INSERT ... ON CONFLICT`, or a parallel aggregation -- ultimately flows through
the executor.

## The Volcano/Iterator Model

PostgreSQL implements the **Volcano** (also called "iterator") execution model.
The core idea is simple and powerful:

- Every plan node exposes the same interface: **return one tuple at a time**.
- A parent node **pulls** tuples from its children by calling `ExecProcNode()`.
- Each call returns one `TupleTableSlot` (or NULL to signal end-of-scan).
- Nodes compose freely into arbitrarily deep trees.

This demand-driven, pull-based design means that a Limit node can stop asking
for tuples after it has enough, a Sort node can buffer all its input before
returning any, and a NestLoop can rescan its inner child for each outer tuple --
all without any node needing to know what its parent or children actually are.

## Lifecycle Protocol

Every query execution follows a strict four-phase protocol:

| Phase | Function | Purpose |
|-------|----------|---------|
| **Start** | `ExecutorStart()` | Build the PlanState tree, open relations, register snapshots |
| **Run** | `ExecutorRun()` | Pull tuples from the root node; may be called multiple times (cursors) |
| **Finish** | `ExecutorFinish()` | Fire AFTER triggers, process modifying CTEs |
| **End** | `ExecutorEnd()` | Recursively clean up all nodes, free all memory |

Each phase provides a hook mechanism for extensions (e.g., `pg_stat_statements`,
`auto_explain`).

## Key Data Structures

| Structure | Role |
|-----------|------|
| `QueryDesc` | Bridge between the traffic cop and executor; carries the plan, snapshots, and destination |
| `EState` | Per-query execution state; owns the query memory context, range table, and parameters |
| `PlanState` | Runtime counterpart of a Plan node; holds the `ExecProcNode` function pointer |
| `TupleTableSlot` | Universal tuple container; supports four storage formats through virtual dispatch |
| `ExprState` | Compiled expression; flat array of evaluation steps with interpreter or JIT dispatch |
| `ExprContext` | Runtime environment for expressions; provides tuple slots and per-tuple memory context |

## Key Design Trade-offs

1. **Tuple-at-a-time vs batch**: The Volcano model processes one tuple per call.
   This simplifies node composition but introduces per-tuple function call
   overhead. PostgreSQL mitigates this with inline dispatch and JIT compilation
   rather than adopting a vectorized model.

2. **Interpreted vs compiled expressions**: Expressions are compiled into a flat
   step array at initialization time. At runtime, either a step interpreter
   (using computed goto on GCC) or a JIT-compiled native function evaluates them.
   The step-based design offers good cache locality compared to recursive tree
   walking.

3. **Memory context hierarchy**: The executor uses a three-level memory hierarchy
   (per-query, per-node, per-tuple) to balance allocation performance against
   memory leak prevention. The per-tuple context is reset on every tuple cycle,
   keeping memory consumption bounded for long-running queries.

4. **Function pointer dispatch**: `ExecProcNode()` dispatches through a function
   pointer rather than a switch statement, eliminating per-tuple dispatch overhead.
   The switch is used only during initialization and cleanup (once per node).

## Scale of the Subsystem

- **70+ source files** in `src/backend/executor/`
- **43 plan node types** across 8 categories (scan, join, aggregation,
  modification, control, parallel, materialization, auxiliary)
- **120+ key symbols** documented across 28 chapters
- **~4,500 lines** in expression compilation alone (`execExpr.c`)

## Reading Roadmap

| Goal | Recommended Path |
|------|-----------------|
| Understand the big picture | Chapters 01-02 |
| Trace a SELECT from start to finish | Chapters 03-04, then 08 |
| Understand expression evaluation | Chapters 06-07 |
| Work on a specific plan node | Chapter 04 for context, then the relevant node chapter (10-19) |
| Debug memory issues | Chapter 07 |
| Understand parallel query | Chapter 13 |
| Quick function lookup | Appendix A (Symbol Index) or API Reference |
