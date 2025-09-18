# try_complete_steps

## Location
src/test/isolation/isolationtester.c: 755 - 817

## Overview
Manages the completion of multiple waiting isolation test steps by checking each one and removing completed steps from the waiting array.

## Definition


## Detailed Description
This function is a core component of PostgreSQL's isolation testing framework that manages the execution and completion of test steps. It iterates through an array of waiting steps, attempting to complete each one using . The function implements a retry mechanism to handle blocker conditions - if any waiting steps have blockers and either step completions or NOTICEs occur, it repeats the completion check loop to ensure consistent timing behavior regardless of step ordering in the array.

The function removes completed steps from the waiting array by shifting remaining elements, maintaining a compact array of only unfinished steps. It continues processing until no further progress can be made (no completions and no new notices when blockers are present).

## Parameters / Member Variables
- : Pointer to the test specification containing session and step information
- : Array of pointers to PermutationStep structures representing steps waiting for completion
- : Current number of steps in the waiting array
- : Control flags passed to  to modify completion behavior

## Dependencies
- Functions called/Symbols referenced:
  - try_complete_step
  - memmove (standard library)
  - TestSpec (structure)
  - PermutationStep (structure)
- Called from (representative examples):
  - run_permutation
  - STEP_RETRY flag usage context

## Notes and Other Information
- The function implements a sophisticated retry mechanism to handle race conditions in step completion
- Uses the global  flag to detect when NOTICEs affect blocker conditions
- Maintains the waiting array in a compacted state by removing completed steps
- Critical for ensuring deterministic behavior in isolation tests regardless of step execution order
- Part of the isolation testing infrastructure used for testing PostgreSQL's transaction isolation levels