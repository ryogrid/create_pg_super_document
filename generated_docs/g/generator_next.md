# generator_next

## Location
[src/backend/statistics/mvdistinct.c:627-641](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/mvdistinct.c#L627-L641)

## Overview
Returns the next combination from the prebuilt list of combinations.

## Definition
```c
static int *generator_next(CombinationGenerator *state)
```

## Detailed Description
This function iterates through the pre-generated combinations stored in a CombinationGenerator structure. It returns a pointer to the next combination of K array indexes (ranging from 0 to N as specified during generator_init), or NULL when all combinations have been exhausted. The function uses the current index to track position in the combinations array and increments it after returning each combination. Each combination is stored as a contiguous block of k integers in the combinations array.

## Parameters / Member Variables
- `state`: Pointer to the CombinationGenerator structure containing pre-generated combinations and current iteration state

## Dependencies
- Functions called/Symbols referenced:
  - [CombinationGenerator](../C/CombinationGenerator.md) (structure type)
- Called from (representative examples):
  - statext_ndistinct_build

## Notes and Other Information
- The function is declared as static, limiting scope to mvdistinct.c
- Returns NULL when all combinations have been iterated through (current == ncombinations)
- Uses pointer arithmetic to access the correct combination in the flat array: &state->combinations[state->k * state->current++]
- The post-increment operator (current++) ensures the index advances after returning the current combination
- Each combination consists of k consecutive integers in the combinations array
- No bounds checking beyond the end-of-combinations check, relying on proper generator initialization
- Located in src/backend/statistics/mvdistinct.c, used for multivariate distinct value statistics
- Designed for simple iteration pattern: call repeatedly until NULL is returned