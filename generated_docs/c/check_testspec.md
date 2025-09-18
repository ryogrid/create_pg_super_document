# check_testspec

## Location
src/test/isolation/isolationtester.c: 252 - 393

## Overview
Validates the test specification and establishes cross-links between nodes in the isolation tester framework.

## Definition


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
  - pg_malloc (memory allocation)
  - qsort (sorting step array)
  - step_qsort_cmp (comparison function for sorting steps)
  - bsearch (binary search for step lookup)
  - step_bsearch_cmp (comparison function for binary search)
  - fprintf/stderr (error reporting)
  - exit (program termination on validation errors)
  - free (memory deallocation)
- Data structures used:
  - TestSpec (main test specification structure)
  - Step (individual test step)
  - Session (session containing steps)
  - Permutation (sequence of steps)
  - PermutationStep (step within a permutation)
  - PermutationStepBlocker (blocking dependency)
  - PSB_ONCE (blocker type constant)
- Called from:
  - main (in isolationtester.c)

## Notes and Other Information
- Function terminates the program with exit(1) if validation errors are found
- Creates a temporary sorted array of all steps for efficient validation operations
- Supports both manually-specified and automatically-generated permutations
- Validates that blocking dependencies don't create intra-session blocks (steps cannot block on their own session)
- Provides detailed error messages for various validation failures
- Memory cleanup is performed by freeing the temporary allsteps array
- The  field in steps is utilized to track which steps are referenced in permutations