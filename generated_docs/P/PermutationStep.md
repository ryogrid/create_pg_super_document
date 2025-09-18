# PermutationStep

## Location
[src/test/isolation/isolationtester.h:67-72](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/isolation/isolationtester.h#L67-L72)

## Overview
A structure that represents a single step within a permutation in the PostgreSQL isolation testing framework, linking to a test step and managing its execution blockers.

## Definition
```c
typedef struct
{
    char       *name;           /* name of referenced Step */
    PermutationStepBlocker **blockers;
    int         nblockers;
    /* These fields are filled by check_testspec(): */
    Step       *step;           /* link to referenced Step */
} PermutationStep;
```

## Detailed Description
PermutationStep is a core data structure in PostgreSQL's isolation testing framework, used to represent individual steps within test permutations. Each PermutationStep references a named Step and contains information about what conditions must be met before this step can execute (blockers). The structure is used during test execution to coordinate the ordering and synchronization of concurrent operations across multiple database sessions.

## Parameters / Member Variables
- `name`: String identifier referencing the name of the Step to be executed
- `blockers`: Array of pointers to PermutationStepBlocker structures that define conditions preventing this step from executing
- `nblockers`: Count of blocker conditions associated with this step
- `step`: Pointer to the actual Step structure, populated during test specification validation by check_testspec()

## Dependencies
- Functions called/Symbols referenced:
  - PermutationStepBlocker
  - [Step](../S/Step.md)
- Called from (representative examples):
  - [check_testspec](../c/check_testspec.md)
  - [run_all_permutations](../r/run_all_permutations.md)
  - [run_permutation](../r/run_permutation.md)
  - [try_complete_steps](../t/try_complete_steps.md)
  - [step_has_blocker](../s/step_has_blocker.md)

## Notes and Other Information
- Part of the isolation testing framework located in src/test/isolation/
- The step field is filled during test validation phase by check_testspec()
- Used extensively in permutation execution to manage step ordering and blocking conditions
- Essential for coordinating concurrent test execution across multiple database sessions
- Works in conjunction with Permutation and TestSpec structures to define complete test scenarios