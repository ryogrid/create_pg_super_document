# check_testspec

## Location
[src/test/isolation/isolationtester.c:252-393](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/isolation/isolationtester.c#L252-L393)

## Overview
Validates the test specification and establishes cross-links between nodes in the isolation tester framework.

## Definition

```c
static void
check_testspec(TestSpec *testspec)
```
## Detailed Description
The  function performs comprehensive validation and setup of a test specification in PostgreSQL's isolation testing framework. This function is responsible for:

1. **Step Collection and Sorting**: Gathers all steps from all sessions and creates a sorted lookup table for efficient searching
2. **Uniqueness Validation**: Ensures all step names are unique across the entire test specification
3. **Session Index Assignment**: Sets session index fields in steps to establish which session each step belongs to
4. **Permutation Linking**: For manually-specified permutations, links PermutationSteps to their corresponding Steps
5. **Blocker Resolution**: Resolves and validates step blocking dependencies, ensuring blockers reference valid steps and don't create invalid dependencies (like steps blocking on their own session)
6. **Usage Verification**: For manual permutations, warns about any defined steps that are not actually used

The function serves as a critical validation phase that ensures the test specification is internally consistent and properly structured before test execution begins. It catches common errors like duplicate step names, undefined step references, and invalid blocking relationships.

## Parameters / Member Variables
- : Pointer to the TestSpec structure containing the complete test specification to validate

## Dependencies
- Functions called/Symbols referenced:
  - [pg_malloc](../p/pg_malloc.md) (memory allocation)
  - qsort (sorting step array)
  - [step_qsort_cmp](../s/step_qsort_cmp.md) (comparison function for sorting steps)
  - bsearch (binary search for step lookup)
  - [step_bsearch_cmp](../s/step_bsearch_cmp.md) (comparison function for binary search)
  - fprintf/stderr (error reporting)
  - exit (program termination on validation errors)
  - free (memory deallocation)
- Data structures used:
  - [TestSpec](../T/TestSpec.md) (main test specification structure)
  - [Step](../S/Step.md) (individual test step)
  - [Session](../S/Session.md) (session containing steps)
  - [Permutation](../P/Permutation.md) (sequence of steps)
  - [PermutationStep](../P/PermutationStep.md) (step within a permutation)
  - PermutationStepBlocker (blocking dependency)
  - PSB_ONCE (blocker type constant)
- Called from:
  - [main](../m/main.md) (in isolationtester.c)

## Notes and Other Information
- Function terminates the program with exit(1) if validation errors are found
- Creates a temporary sorted array of all steps for efficient validation operations
- Supports both manually-specified and automatically-generated permutations
- Validates that blocking dependencies don't create intra-session blocks (steps cannot block on their own session)
- Provides detailed error messages for various validation failures
- Memory cleanup is performed by freeing the temporary allsteps array
- The  field in steps is utilized to track which steps are referenced in permutations