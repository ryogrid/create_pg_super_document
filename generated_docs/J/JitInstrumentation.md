# JitInstrumentation

## Location
src/include/jit/jit.h: 27 - 46

## Overview
JitInstrumentation is a structure that tracks performance metrics and timing information for PostgreSQL's Just-In-Time (JIT) compilation system.

## Definition


## Detailed Description
JitInstrumentation serves as a comprehensive performance monitoring structure for PostgreSQL's JIT compilation subsystem. It collects detailed timing and counting metrics for various phases of the JIT compilation process, enabling performance analysis and optimization of the JIT system. The structure is used throughout the executor and explain functionality to provide insights into JIT compilation overhead and effectiveness.

## Parameters / Member Variables
- : Counter tracking the total number of functions that have been JIT-compiled and emitted
- : Accumulated time spent in the overall code generation process
- : Time spent specifically on tuple deformation operations (subset of generation_counter)
- : Time spent on function inlining optimizations during compilation
- : Time spent on various optimization passes during compilation
- : Time spent on the final code emission phase

## Dependencies
- Functions called/Symbols referenced:
  - instr_time (timing instrumentation type)
- Called from (representative examples):
  - ExplainPrintJITSummary
  - ExplainPrintJIT
  - ExecParallelRetrieveJitInstrumentation
  - InstrJitAgg

## Notes and Other Information
- The deform_counter is explicitly noted as being included within generation_counter, indicating a hierarchical timing relationship
- Used extensively in EXPLAIN functionality to report JIT compilation performance
- Integrated with parallel execution infrastructure for collecting JIT metrics across worker processes
- Part of the broader JIT instrumentation framework that helps DBAs understand JIT compilation costs and benefits