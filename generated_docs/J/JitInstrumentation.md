# JitInstrumentation

## Location
[src/include/jit/jit.h:27-46](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/jit/jit.h#L27-L46)

## Overview
JitInstrumentation is a structure that tracks performance metrics and timing information for PostgreSQL's Just-In-Time (JIT) compilation system.

## Definition

```c
typedef struct JitInstrumentation
{
	/* number of emitted functions */
	size_t		created_functions;

	/* accumulated time to generate code */
	instr_time	generation_counter;

	/* accumulated time to deform tuples, included into generation_counter */
	instr_time	deform_counter;

	/* accumulated time for inlining */
	instr_time	inlining_counter;

	/* accumulated time for optimization */
	instr_time	optimization_counter;

	/* accumulated time for code emission */
	instr_time	emission_counter;
} JitInstrumentation;
```
## Detailed Description
JitInstrumentation serves as a comprehensive performance monitoring structure for PostgreSQL's JIT compilation subsystem. It collects detailed timing and counting metrics for various phases of the JIT compilation process, enabling performance analysis and optimization of the JIT system. The structure is used throughout the executor and explain functionality to provide insights into JIT compilation overhead and effectiveness.

## Parameters / Member Variables
- `created_functions`: Counter tracking the total number of functions that have been JIT-compiled and emitted
- `generation_counter`: Accumulated time spent in the overall code generation process
- `deform_counter`: Time spent specifically on tuple deformation operations (subset of generation_counter)
- `inlining_counter`: Time spent on function inlining optimizations during compilation
- `optimization_counter`: Time spent on various optimization passes during compilation
- `emission_counter`: Time spent on the final code emission phase
## Dependencies
- Functions called/Symbols referenced:
  - [instr_time](../i/instr_time.md) (timing instrumentation type)
- Called from (representative examples):
  - [ExplainPrintJITSummary](../E/ExplainPrintJITSummary.md)
  - [ExplainPrintJIT](../E/ExplainPrintJIT.md)
  - [ExecParallelRetrieveJitInstrumentation](../E/ExecParallelRetrieveJitInstrumentation.md)
  - [InstrJitAgg](../I/InstrJitAgg.md)

## Notes and Other Information
- The deform_counter is explicitly noted as being included within generation_counter, indicating a hierarchical timing relationship
- Used extensively in EXPLAIN functionality to report JIT compilation performance
- Integrated with parallel execution infrastructure for collecting JIT metrics across worker processes
- Part of the broader JIT instrumentation framework that helps DBAs understand JIT compilation costs and benefits