# Step

## Location
src/test/isolation/isolationtester.h: 22 - 30

## Overview
Step is a struct that represents an individual test step in PostgreSQL's isolation testing framework, containing the step name, SQL command, and metadata for test execution.

## Definition


## Detailed Description
The Step struct is a fundamental component of PostgreSQL's isolation testing system, which is used to test concurrent transaction behavior and isolation levels. Each Step represents a single SQL operation that can be executed as part of a test scenario. The isolation tester uses these steps to create various permutations of concurrent operations to verify that PostgreSQL maintains proper isolation guarantees.

The struct is part of the isolation test specification language defined in specparse.y, and steps are organized within sessions and executed in different permutations to test various concurrency scenarios. The isolation tester framework allows developers to write declarative tests that verify transaction isolation behavior without having to manually coordinate complex timing scenarios.

## Parameters / Member Variables
- : A unique identifier string for the step, used for referencing in permutations and blocking specifications
- : The SQL command or statement that this step will execute when run
- : An integer identifying which session (connection) owns this step, filled during test specification validation
- : A boolean flag indicating whether this step has been referenced in any permutation, used for validation and optimization

## Dependencies
- Functions called/Symbols referenced:
  - Used within Session struct as an array of Step pointers
  - Referenced by PermutationStepBlocker and PermutationStep structs

- Called from (representative examples):
  - [check_testspec](../c/check_testspec.md): Validates and processes step definitions
  - [step_qsort_cmp](../s/step_qsort_cmp.md): Comparison function for sorting steps by name
  - [step_bsearch_cmp](../s/step_bsearch_cmp.md): Comparison function for binary search lookup
  - [run_permutation](../r/run_permutation.md): Executes steps in specified order
  - [try_complete_step](../t/try_complete_step.md): Attempts to complete a step execution

## Notes and Other Information
- [Step](Step.md) structs are typically allocated and populated by the spec parser (specparse.y)
- The isolation tester creates a sorted lookup table of all steps for efficient name-based lookups
- [Step](Step.md) names must be unique across all sessions within a test specification
- The 'used' flag helps identify unreferenced steps during test validation
- Steps are executed asynchronously across multiple database connections to simulate real-world concurrency scenarios
- This is part of PostgreSQL's comprehensive testing infrastructure specifically designed for verifying ACID properties and isolation levels