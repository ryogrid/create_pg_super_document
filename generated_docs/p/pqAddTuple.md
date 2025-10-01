# pqAddTuple

## Location
[src/interfaces/libpq/fe-exec.c:993-1059](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L993-L1059)

## Overview
pqAddTuple is a private utility function that adds a row pointer to a PGresult structure, automatically growing the tuple array when necessary to accommodate new tuples.

## Definition

```c
structure is
		 * okay. Note that the first time through, res->tuples is NULL. While
		 * ANSI says that realloc() should act like malloc() in that case,
		 * some old C libraries (like SunOS 4.1.x) coredump instead. On
		 * failure realloc is supposed to return NULL without damaging the
		 * existing allocation. Note that the positions beyond res->ntups are
		 * garbage, not necessarily NULL.
		 */
		int			newSize;
```
## Detailed Description
This function manages the dynamic growth of the tuple array within a PGresult structure. When adding a new tuple would exceed the current array capacity, it automatically reallocates memory to double the array size (or sets it to 128 for the initial allocation). The function implements several safety checks including overflow protection for both row count limits (INT_MAX) and memory size limits (SIZE_MAX on 32-bit platforms).

The function uses realloc() for memory expansion, with special handling for the initial allocation case where res->tuples is NULL. It updates the memory accounting in res->memorySize and maintains the integrity of the tuple array structure.

## Parameters / Member Variables
- : Pointer to the PGresult structure that will receive the new tuple
- : Pointer to the PGresAttValue array representing the tuple to be added
- : Double pointer for returning error messages; set to NULL to use default "out of memory" message

## Dependencies
- Functions called/Symbols referenced:
  - [libpq_gettext](../l/libpq_gettext.md) (for error message localization)
  - malloc (for initial array allocation)
  - realloc (for array expansion)
- Types used:
  - PGresAttValue (tuple attribute value type)
- Constants used:
  - INT_MAX (maximum row count limit)
  - SIZE_MAX (memory size limit on 32-bit platforms)
- Called from:
  - [PQsetvalue](../P/PQsetvalue.md)
  - [pqRowProcessor](pqRowProcessor.md)

## Notes and Other Information
- The function is marked as static, indicating it's for internal libpq use only
- Implements exponential growth strategy: doubles array size when expansion is needed, starting with 128 entries
- Includes platform-specific overflow checks for 32-bit systems where SIZE_MAX might be exceeded before INT_MAX
- Handles the special case where realloc() might not behave like malloc() on some older C libraries (like SunOS 4.1.x)
- Updates memory accounting (res->memorySize) to track total allocated memory
- Returns false on allocation failure or overflow conditions, true on success
- The tuple array positions beyond res->ntups contain garbage data, not necessarily NULL
- Enforces a hard limit of INT_MAX tuples per result set since row numbers use integers

## Simplified Source

```c
static bool pqAddTuple(PGresult *res, PGresAttValue *tup, const char **errmsgp) {
    // Check if we need to grow the tuple array
    if (res->ntups >= res->tupArrSize) {
        int newSize;
        PGresAttValue **newTuples;

        // Calculate new size: double current size or start with 128
        if (res->tupArrSize <= INT_MAX / 2)
            newSize = (res->tupArrSize > 0) ? res->tupArrSize * 2 : 128;
        else if (res->tupArrSize < INT_MAX)
            newSize = INT_MAX;
        else {
            *errmsgp = libpq_gettext("PGresult cannot support more than INT_MAX tuples");
            return false;
        }

        // Check for size_t overflow on 32-bit platforms
        #if INT_MAX >= (SIZE_MAX / 2)
        if (newSize > SIZE_MAX / sizeof(PGresAttValue *)) {
            *errmsgp = libpq_gettext("size_t overflow");
            return false;
        }
        #endif

        // Allocate or reallocate memory
        if (res->tuples == NULL)
            newTuples = malloc(newSize * sizeof(PGresAttValue *));
        else
            newTuples = realloc(res->tuples, newSize * sizeof(PGresAttValue *));

        if (!newTuples)
            return false;  // Memory allocation failed

        // Update memory tracking and array size
        res->memorySize += (newSize - res->tupArrSize) * sizeof(PGresAttValue *);
        res->tupArrSize = newSize;
        res->tuples = newTuples;
    }

    // Add the new tuple and increment count
    res->tuples[res->ntups] = tup;
    res->ntups++;
    return true;
}
```