# generator_free

## Location
[src/backend/statistics/mvdistinct.c:642-656](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/mvdistinct.c#L642-L656)

## Overview
Frees the internal state of the combination generator and releases allocated memory.

## Definition
```c
static void generator_free(CombinationGenerator *state)
```

## Detailed Description
This function deallocates the memory used by a CombinationGenerator structure, including both the pre-built combinations array and the generator state itself. It performs cleanup in the proper order by first freeing the combinations array, then freeing the generator structure. This function should be called when the generator is no longer needed to prevent memory leaks.

## Parameters / Member Variables
- `state`: Pointer to the CombinationGenerator structure to be freed

## Dependencies
- Functions called/Symbols referenced:
  - [CombinationGenerator](../C/CombinationGenerator.md) (structure type)
  - [pfree](../p/pfree.md) (PostgreSQL memory deallocation function)
- Called from (representative examples):
  - [statext_ndistinct_build](../s/statext_ndistinct_build.md)

## Notes and Other Information
- The function is declared as static, limiting scope to mvdistinct.c
- Uses PostgreSQL's pfree function to deallocate memory allocated with palloc
- Frees memory in the correct order: first the combinations array, then the structure itself
- No return value since this is a cleanup function
- Should be called exactly once per generator after use to avoid memory leaks
- No null pointer checks - assumes valid state pointer (typical for internal functions)
- Located in src/backend/statistics/mvdistinct.c, part of the multivariate distinct value statistics system
- Complements generator_init as the cleanup counterpart in the generator lifecycle

## Simplified Source

```c
static void
generator_free(CombinationGenerator *state)
{
    // Free the combinations array
    pfree(state->combinations);

    // Free the generator structure itself
    pfree(state);
}
```