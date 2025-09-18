# SharedJitInstrumentation

## Location
src/include/jit/jit.h: 51 - 55

## Overview
SharedJitInstrumentation is a Dynamic Shared Memory (DSM) structure designed to accumulate JIT instrumentation data from all parallel worker processes.

## Definition


## Detailed Description
SharedJitInstrumentation serves as a coordination structure for collecting JIT compilation performance metrics from parallel worker processes in PostgreSQL's parallel execution framework. It uses Dynamic Shared Memory to allow multiple worker processes to report their JIT instrumentation data to a central location, which can then be aggregated and reported by the leader process. The structure employs a flexible array member to accommodate a variable number of worker processes.

## Parameters / Member Variables
- : The total number of parallel worker processes that will contribute JIT instrumentation data
- : A flexible array containing JitInstrumentation structures, one for each worker process

## Dependencies
- Functions called/Symbols referenced:
  - JitInstrumentation (embedded structure for each worker)
  - FLEXIBLE_ARRAY_MEMBER (PostgreSQL macro for variable-length arrays)
- Called from (representative examples):
  - ExecInitParallelPlan
  - ExecParallelRetrieveJitInstrumentation
  - ParallelQueryMain
  - ExplainNode

## Notes and Other Information
- Specifically designed for Dynamic Shared Memory (DSM) usage in parallel query execution
- The flexible array member allows the structure to scale with the number of parallel workers
- Used in the parallel execution infrastructure to collect and aggregate JIT metrics across all worker processes
- Essential for providing accurate JIT performance reporting in EXPLAIN output for parallel queries
- Part of the broader parallel execution instrumentation framework in PostgreSQL