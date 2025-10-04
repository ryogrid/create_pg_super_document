# try_complete_steps

## Location
[src/test/isolation/isolationtester.c:755-817](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/isolation/isolationtester.c#L755-L817)

## Overview
Manages the completion of multiple waiting isolation test steps by checking each one and removing completed steps from the waiting array.

## Definition

```c
struct timeval start_time;
```
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
  - [try_complete_step](try_complete_step.md)
  - memmove (standard library)
  - [TestSpec](../T/TestSpec.md) (structure)
  - [PermutationStep](../P/PermutationStep.md) (structure)
- Called from (representative examples):
  - [run_permutation](../r/run_permutation.md)
  - STEP_RETRY flag usage context

## Notes and Other Information
- The function implements a sophisticated retry mechanism to handle race conditions in step completion
- Uses the global  flag to detect when NOTICEs affect blocker conditions
- Maintains the waiting array in a compacted state by removing completed steps
- Critical for ensuring deterministic behavior in isolation tests regardless of step execution order
- Part of the isolation testing infrastructure used for testing PostgreSQL's transaction isolation levels

## Simplified Source

```c
static int
try_complete_steps(TestSpec *testspec, PermutationStep **waiting,
                   int nwaiting, int flags)
{
    int old_nwaiting;
    bool have_blocker;

    do {
        int w = 0;
        any_new_notice = false;  // Reset notice flag
        old_nwaiting = nwaiting;
        have_blocker = false;

        // Try to complete each waiting step
        while (w < nwaiting) {
            if (try_complete_step(testspec, waiting[w], flags)) {
                // Step still blocked
                if (waiting[w]->nblockers > 0)
                    have_blocker = true;
                w++;
            } else {
                // Step completed - remove from waiting array
                if (w + 1 < nwaiting)
                    memmove(&waiting[w], &waiting[w + 1],
                           (nwaiting - (w + 1)) * sizeof(PermutationStep *));
                nwaiting--;
            }
        }

        // Retry if blockers exist and progress was made
    } while (have_blocker && (nwaiting < old_nwaiting || any_new_notice));

    return nwaiting;
}
```