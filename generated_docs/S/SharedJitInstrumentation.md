# SharedJitInstrumentation

## Location
[src/include/jit/jit.h:51-55](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/jit/jit.h#L51-L55)

## Overview
SharedJitInstrumentation is a Dynamic Shared Memory (DSM) structure designed to accumulate JIT instrumentation data from all parallel worker processes.

## Definition

```c
typedef struct SharedJitInstrumentation
{
	int			num_workers;
	JitInstrumentation jit_instr[FLEXIBLE_ARRAY_MEMBER];
} SharedJitInstrumentation;
```
## Detailed Description
SharedJitInstrumentation serves as a coordination structure for collecting JIT compilation performance metrics from parallel worker processes in PostgreSQL's parallel execution framework. It uses Dynamic Shared Memory to allow multiple worker processes to report their JIT instrumentation data to a central location, which can then be aggregated and reported by the leader process. The structure employs a flexible array member to accommodate a variable number of worker processes.

## Parameters / Member Variables
- : The total number of parallel worker processes that will contribute JIT instrumentation data
- : A flexible array containing JitInstrumentation structures, one for each worker process

## Dependencies
- Functions called/Symbols referenced:
  - [JitInstrumentation](../J/JitInstrumentation.md) (embedded structure for each worker)
  - FLEXIBLE_ARRAY_MEMBER (PostgreSQL macro for variable-length arrays)
- Called from (representative examples):
  - [ExecInitParallelPlan](../E/ExecInitParallelPlan.md)
  - [ExecParallelRetrieveJitInstrumentation](../E/ExecParallelRetrieveJitInstrumentation.md)
  - [ParallelQueryMain](../P/ParallelQueryMain.md)
  - [ExplainNode](../E/ExplainNode.md)

## Notes and Other Information
- Specifically designed for Dynamic Shared Memory (DSM) usage in parallel query execution
- The flexible array member allows the structure to scale with the number of parallel workers
- Used in the parallel execution infrastructure to collect and aggregate JIT metrics across all worker processes
- Essential for providing accurate JIT performance reporting in EXPLAIN output for parallel queries
- Part of the broader parallel execution instrumentation framework in PostgreSQL