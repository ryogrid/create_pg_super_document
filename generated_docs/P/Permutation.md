# Permutation

## Location
[src/test/isolation/isolationtester.h:73-83](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/isolation/isolationtester.h#L73-L83)

## Overview
A structure that represents a specific ordering of test steps in the PostgreSQL isolation testing framework, defining a sequence of PermutationSteps to be executed.

## Definition
```c
typedef struct
{
    int             nsteps;
    PermutationStep **steps;
} Permutation;
```

## Detailed Description
Permutation is a fundamental data structure in PostgreSQL's isolation testing framework that defines a specific sequence of test steps to be executed. Each permutation represents one possible ordering of the test steps, allowing the isolation tester to systematically explore different execution patterns to verify database behavior under concurrent access. The structure contains an array of PermutationStep pointers that define the exact order in which steps should be attempted during test execution.

## Parameters / Member Variables
- `nsteps`: The number of steps in this permutation
- `steps`: Array of pointers to PermutationStep structures, defining the ordered sequence of steps to execute

## Dependencies
- Functions called/Symbols referenced:
  - [PermutationStep](PermutationStep.md)
- Called from (representative examples):
  - [check_testspec](../c/check_testspec.md)
  - [run_named_permutations](../r/run_named_permutations.md)

## Notes and Other Information
- Core component of the isolation testing framework in src/test/isolation/
- Used to systematically test different execution orderings of database operations
- Works in conjunction with TestSpec to define complete test scenarios
- Each permutation represents one possible interleaving of concurrent operations
- Essential for detecting race conditions and isolation violations in PostgreSQL
- Permutations are typically generated or defined in test specification files and validated by check_testspec()