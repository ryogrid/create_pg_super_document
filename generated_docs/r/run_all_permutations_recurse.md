# run_all_permutations_recurse

## Location
[src/test/isolation/isolationtester.c:446-485](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/isolation/isolationtester.c#L446-L485)

## Overview
A recursive function that generates and executes all possible permutations of test steps across multiple sessions in PostgreSQL's isolation testing framework.

## Definition

```c
static void
run_all_permutations_recurse(TestSpec *testspec, int *piles,
							 int nsteps, PermutationStep **steps)
```
## Detailed Description
This function implements the core recursive logic for generating permutations of test steps in the isolation tester. It systematically explores all possible orderings of steps across different sessions by treating each session as a "pile" of steps. The function uses a backtracking approach where it picks one step from any available session, recurses to build the rest of the permutation, then backtracks to try the next possibility.

When all sessions have exhausted their steps (all piles are empty), it executes the completed permutation by calling . This ensures that every possible interleaving of steps across sessions is tested, which is crucial for detecting race conditions and isolation issues in PostgreSQL.

## Parameters / Member Variables
- : Pointer to the test specification containing all sessions and their steps
- : Array tracking the current position (next step to pick) in each session
- : Current number of steps in the permutation being built
- : Array of PermutationStep pointers representing the current permutation

## Dependencies
- Functions called/Symbols referenced:
  - [TestSpec](../T/TestSpec.md) (struct type)
  - [PermutationStep](../P/PermutationStep.md) (struct type) 
  - [Step](../S/Step.md) (struct type)
  - [run_all_permutations_recurse](run_all_permutations_recurse.md) (recursive self-call)
  - [run_permutation](run_permutation.md)
- Called from (representative examples):
  - [run_all_permutations](run_all_permutations.md)
  - [run_all_permutations_recurse](run_all_permutations_recurse.md) (recursive calls)

## Notes and Other Information
- Uses a backtracking algorithm to generate all possible step orderings
- The piles array is modified and restored during recursion to track state
- PermutationSteps created here have no blocker conditions (automatically generated)
- Critical for comprehensive isolation testing in PostgreSQL
- Part of the isolation tester framework located in src/test/isolation/