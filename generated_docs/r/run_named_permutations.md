# run_named_permutations

## Location
[src/test/isolation/isolationtester.c:486-498](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/isolation/isolationtester.c#L486-L498)

## Overview
Executes specific named permutations that are explicitly defined in the test specification rather than generating all possible permutations automatically.

## Definition

```c
static void
run_named_permutations(TestSpec *testspec)
```
## Detailed Description
This function runs predefined permutations that are explicitly specified in the test configuration. Unlike  which generates all possible step orderings automatically, this function executes only the specific permutations that have been manually defined in the test specification. This allows test writers to focus on particular scenarios or problematic interleavings without running the full combinatorial explosion of all possibilities.

The function iterates through the array of predefined permutations in the test spec and executes each one by calling  with the pre-arranged sequence of steps.

## Parameters / Member Variables
- `*testspec`: Pointer to the test specification containing the named permutations to execute
## Dependencies
- Functions called/Symbols referenced:
  - [TestSpec](../T/TestSpec.md) (struct type)
  - [Permutation](../P/Permutation.md) (struct type)
  - [run_permutation](run_permutation.md)
- Called from (representative examples):
  - [run_testspec](run_testspec.md)

## Notes and Other Information
- Provides a more targeted testing approach compared to exhaustive permutation generation
- Allows test authors to specify exactly which step orderings to test
- More efficient than full permutation testing when only specific scenarios need validation
- Part of the isolation testing framework for PostgreSQL concurrency testing
- Complements the automatic permutation generation functionality

## Simplified Source

```c
static void
run_named_permutations(TestSpec *testspec)
{
    // Execute each predefined permutation in the test spec
    for (int i = 0; i < testspec->npermutations; i++) {
        Permutation *p = testspec->permutations[i];
        run_permutation(testspec, p->nsteps, p->steps);
    }
}
```