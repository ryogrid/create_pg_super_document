# run_permutation

## Location
[src/test/isolation/isolationtester.c:520-754](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/isolation/isolationtester.c#L520-L754)

## Overview
Executes a single permutation of test steps across multiple database sessions, managing concurrent execution, blocking conditions, and proper setup/teardown procedures.

## Definition

```c
struct timeval start_time;
```
## Detailed Description
This function is the core execution engine for individual test permutations in PostgreSQL's isolation testing framework. It orchestrates the execution of a specific sequence of test steps across multiple database sessions, handling the complex coordination required for concurrent testing scenarios.

The function operates in several phases:
1. **Setup Phase**: Executes global setup SQL and per-session setup SQL
2. **Execution Phase**: Executes the test steps in the specified order, managing concurrent execution and blocking conditions
3. **Coordination Phase**: Handles step dependencies, timeouts, and completion detection
4. **Teardown Phase**: Performs cleanup with per-session and global teardown SQL

Key features include asynchronous step execution using PQsendQuery, sophisticated blocking condition handling with timeout mechanisms, step completion tracking across multiple sessions, and comprehensive error handling and reporting.

## Parameters / Member Variables
- : Pointer to the test specification containing all test metadata and configuration
- : Number of steps in this permutation
- : Array of PermutationStep pointers defining the step sequence to execute

## Dependencies
- Functions called/Symbols referenced:
  - [TestSpec](../T/TestSpec.md), PermutationStep, Step, IsoConnInfo, PermutationStepBlocker (struct types)
  - pg_malloc, PQexec, PQsendQuery, gettimeofday (standard/PostgreSQL functions)
  - [printResultSet](../p/printResultSet.md), try_complete_step, try_complete_steps (helper functions)
  - PGRES_TUPLES_OK, PGRES_COMMAND_OK, PSB_NUM_NOTICES (constants)
  - STEP_RETRY, STEP_NONBLOCK, USECS_PER_SEC (constants/macros)
- Called from (representative examples):
  - [run_all_permutations_recurse](run_all_permutations_recurse.md)
  - [run_named_permutations](run_named_permutations.md)

## Notes and Other Information
- Handles complex concurrency scenarios with multiple database sessions
- Implements timeout mechanisms to prevent indefinite blocking
- Manages blocker conditions that can prevent step completion
- Uses asynchronous PostgreSQL query execution for realistic concurrency testing
- Critical component for detecting race conditions and isolation violations
- Provides detailed error reporting and debugging information
- Part of PostgreSQL's comprehensive isolation testing infrastructure
- Handles both successful completion and error scenarios gracefully