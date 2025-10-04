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
  - [pg_malloc](../p/pg_malloc.md), PQexec, PQsendQuery, gettimeofday (standard/PostgreSQL functions)
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

## Simplified Source

```c
static void
run_permutation(TestSpec *testspec, int nsteps, PermutationStep **steps)
{
    PermutationStep **waiting = pg_malloc(sizeof(PermutationStep *) * testspec->nsessions);
    int nwaiting = 0;

    printf("\nstarting permutation:");
    for (int i = 0; i < nsteps; i++)
        printf(" %s", steps[i]->name);
    printf("\n");

    // Execute setup SQL
    for (int i = 0; i < testspec->nsetupsqls; i++) {
        PGresult *res = PQexec(conns[0].conn, testspec->setupsqls[i]);
        if (PQresultStatus(res) == PGRES_TUPLES_OK)
            printResultSet(res);
        else if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            fprintf(stderr, "setup failed: %s", PQerrorMessage(conns[0].conn));
            exit(1);
        }
        PQclear(res);
    }

    // Execute per-session setup
    for (int i = 0; i < testspec->nsessions; i++) {
        if (testspec->sessions[i]->setupsql) {
            // Execute session setup SQL (similar error handling)
        }
    }

    // Execute the test steps
    for (int i = 0; i < nsteps; i++) {
        PermutationStep *pstep = steps[i];
        Step *step = pstep->step;
        IsoConnInfo *iconn = &conns[1 + step->session];

        // Wait for any active step in this session to complete
        if (iconn->active_step != NULL) {
            // Wait with timeout handling
            while (iconn->active_step != NULL) {
                if (!try_complete_step(testspec, iconn->active_step, STEP_RETRY)) {
                    // Remove from waiting array
                    nwaiting = try_complete_steps(testspec, waiting, nwaiting,
                                                STEP_NONBLOCK | STEP_RETRY);
                }
                // Handle timeout scenarios
            }
        }

        // Send query for this step
        if (!PQsendQuery(iconn->conn, step->sql)) {
            fprintf(stdout, "failed to send query for step %s: %s\n",
                   step->name, PQerrorMessage(iconn->conn));
            exit(1);
        }

        iconn->active_step = pstep;

        // Try to complete step and manage waiting list
        bool mustwait = try_complete_step(testspec, pstep, STEP_NONBLOCK);
        nwaiting = try_complete_steps(testspec, waiting, nwaiting,
                                    STEP_NONBLOCK | STEP_RETRY);

        if (mustwait)
            waiting[nwaiting++] = pstep;
    }

    // Wait for any remaining steps to complete
    nwaiting = try_complete_steps(testspec, waiting, nwaiting, STEP_RETRY);
    if (nwaiting != 0) {
        fprintf(stderr, "failed to complete permutation due to mutually-blocking steps\n");
        exit(1);
    }

    // Execute teardown (per-session and global)
    // ... teardown code similar to setup ...

    free(waiting);
}
```