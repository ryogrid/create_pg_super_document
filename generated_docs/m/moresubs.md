# moresubs

## Location
[src/backend/regex/regcomp.c:555-591](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regcomp.c#L555-L591)

## Overview
Enlarges the subRE vector (sub-regular expression array) by reallocating memory to accommodate additional sub-regular expressions during regex compilation.

## Definition

```c
static void
moresubs(struct vars *v,
		 int wanted)			/* want enough room for this one */
```
## Detailed Description
The  function dynamically expands the subRE (sub-regular expression) vector when more storage space is needed during regular expression compilation. It implements a growth strategy that allocates 1.5x the requested size plus one to minimize future reallocations. The function handles two scenarios: initial allocation from a small static array () and subsequent reallocations of an already-allocated dynamic array.

The function first checks if the current  array is pointing to the static  array. If so, it allocates new memory and copies the existing entries. Otherwise, it uses  to expand the existing allocation. After successful allocation, it initializes the new entries to NULL and updates the  counter.

## Parameters / Member Variables
- `*v`: Pointer to the vars structure containing regex compilation state, including the current subRE array and count
- `wanted`: The minimum number of subRE entries needed (must be greater than current )
## Dependencies
- Functions called/Symbols referenced:
  -  - Memory allocation macro
  -  - Memory reallocation macro  
  -  - Memory copying function via VS macro
  -  - Error reporting macro
  -  - Out of memory error constant
  -  - Sub-regular expression structure type
  -  - Void pointer casting macro
- Called from (representative examples):
  -  (src/backend/regex/regcomp.c:1017)

## Notes and Other Information
- Uses a growth factor of 1.5x plus one to balance memory usage and reallocation frequency
- Properly handles transition from static to dynamic allocation
- Initializes new entries to NULL for safety
- Sets REG_ESPACE error and returns on allocation failure
- Includes assertions to verify the wanted parameter and final state consistency

## Simplified Source

```c
static void
moresubs(struct vars *v, int wanted)
{
    struct subre **p;
    size_t n;

    // Calculate new size: 1.5x wanted + 1 for efficiency
    n = (size_t) wanted * 3 / 2 + 1;

    // Check if using static array or already allocated
    if (v->subs == v->sub10) {
        // First allocation: copy from static array
        p = (struct subre **) MALLOC(n * sizeof(struct subre *));
        if (p != NULL)
            memcpy(VS(p), VS(v->subs), v->nsubs * sizeof(struct subre *));
    } else {
        // Reallocate existing dynamic array
        p = (struct subre **) REALLOC(v->subs, n * sizeof(struct subre *));
    }

    // Handle allocation failure
    if (p == NULL) {
        ERR(REG_ESPACE);
        return;
    }

    // Update array pointer and initialize new entries to NULL
    v->subs = p;
    for (p = &v->subs[v->nsubs]; v->nsubs < n; p++, v->nsubs++)
        *p = NULL;
}
```