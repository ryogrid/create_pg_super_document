# freelacons

## Location
[src/backend/regex/regcomp.c:2430-2446](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regcomp.c#L2430-L2446)

## Overview
Deallocates the memory used by a lookaround-constraint sub-regular expression vector, freeing both individual compiled NFAs and the array itself.

## Definition

```c
static void
freelacons(struct subre *subs,
		   int n)
```
## Detailed Description
The freelacons function is responsible for properly cleaning up memory allocated for lookaround constraint structures (lacons) in PostgreSQL's regex engine. It serves as the cleanup counterpart to newlacon, ensuring that all dynamically allocated memory associated with lookaround assertions is properly freed.

The function performs a two-step cleanup process:
1. Iterates through the lacons array (skipping index 0) and frees any compiled NFAs (cnfa structures) that were allocated for individual lookaround constraints
2. Frees the entire lacons array itself

The iteration starts from index 1 because index 0 is intentionally unused in the lacons array design. For each lacon entry, it checks if a compiled NFA exists (using NULLCNFA) and frees it if present. This ensures that no memory leaks occur from partially processed or completed lookaround constraints.

## Parameters / Member Variables
- `*subs`: pointer to the array of subre structures representing the lacons
- `n`: total number of elements in the lacons array (including the unused 0th element)
## Dependencies
- Functions called/Symbols referenced:
  - NULLCNFA - Macro to check if cnfa structure is null/empty
  - [freecnfa](freecnfa.md) - Frees compiled NFA structures
  - FREE - Deallocates main array memory
- Called from (representative examples):
  - [freev](freev.md) - Main vars structure cleanup function
  - [rfree](../r/rfree.md) - Regex structure cleanup function

## Notes and Other Information
- Skips index 0 in the array as it's intentionally unused in the lacon design
- Safely handles partially initialized lacons by checking NULLCNFA before freeing
- Part of the comprehensive memory management system for regex compilation
- Essential for preventing memory leaks in regex patterns with lookaround assertions
- The assert(n > 0) ensures the function is not called with invalid array sizes
- Memory cleanup is performed in reverse dependency order (cnfas first, then array)

## Simplified Source

```c
static void freelacons(struct subre *subs, int n) {
    struct subre *sub;
    int i;

    assert(n > 0);

    // Free individual cnfas (skip index 0 which is unused)
    for (sub = subs + 1, i = n - 1; i > 0; sub++, i--) {
        if (!NULLCNFA(sub->cnfa))
            freecnfa(&sub->cnfa);
    }

    // Free the entire array
    FREE(subs);
}
```