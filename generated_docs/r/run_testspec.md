# run_testspec

## Location
[src/test/isolation/isolationtester.c:394-405](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/isolation/isolationtester.c#L394-L405)

## Overview
Executes the permutations specified in the test specification, choosing between explicitly defined permutations or automatically generated ones.

## Definition


## Detailed Description
The  function serves as a dispatcher that determines the execution strategy for an isolation test based on the test specification configuration. It implements a simple branching logic:

- **Named Permutations**: If the test specification contains explicitly defined permutations (manual test scenarios), it delegates execution to  which will execute only the specified permutation sequences
- **All Permutations**: If no explicit permutations are defined, it delegates to  which will automatically generate and execute all possible permutations of the test steps

This design provides flexibility in the isolation testing framework, allowing test authors to either specify precise test scenarios they want to validate or let the system exhaustively test all possible interleavings of concurrent operations.

## Parameters / Member Variables
- : Pointer to the TestSpec structure containing the complete test specification including sessions, steps, and optional permutations

## Dependencies
- Functions called/Symbols referenced:
  - [run_named_permutations](run_named_permutations.md) (executes explicitly specified permutations)
  - [run_all_permutations](run_all_permutations.md) (generates and executes all possible permutations)
- Data structures used:
  - [TestSpec](../T/TestSpec.md) (main test specification structure)
- Called from:
  - [main](../m/main.md) (in isolationtester.c)

## Notes and Other Information
- Acts as a simple but important routing function in the isolation test execution flow
- The decision between named and all permutations is based on whether  is non-NULL
- This function represents the main execution entry point after test specification validation is complete
- Provides a clean abstraction that separates test specification parsing/validation from test execution strategies
- The choice of execution strategy significantly affects test runtime and coverage (all permutations can be exponentially larger than named permutations)