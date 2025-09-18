# clearcvec

## Location
src/backend/regex/regc_cvec.c: 66 - 78

## Overview
Resets a character vector (cvec) structure to an empty state, clearing all character data and ranges while preserving the allocated memory structure.

## Definition
```c
static struct cvec *clearcvec(struct cvec *cv)
```

## Detailed Description
The `clearcvec` function is a utility function in PostgreSQL's regular expression engine that resets a character vector structure (`cvec`) to an empty state. This function is used to clear the contents of a `cvec` without deallocating its memory, making it ready for reuse. It resets the character count, range count, and character class code to their default empty values. The function returns the same `cvec` pointer as a convenience for chaining operations.

This function is typically used when a `cvec` needs to be reused for a new set of characters or when initializing a newly allocated `cvec` structure. It's more efficient than freeing and reallocating memory when the same `cvec` can be reused.

## Parameters / Member Variables
- `cv`: Pointer to the `cvec` structure to be cleared. Must not be NULL (asserted).

## Dependencies
- Functions called/Symbols referenced:
  - `[cvec](cvec.md)` (structure type)
  - `assert` (for NULL pointer validation)
- Called from (representative examples):
  - `newcvec` (src/backend/regex/regc_cvec.c:58)
  - `getcvec` (src/backend/regex/regc_cvec.c:119)
  - `REPLACEARC` (src/backend/regex/regcomp.c:245)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the same source file (regc_cvec.c)
- The function preserves the allocated memory space (chrspace and rangespace remain unchanged)
- Character class code is reset to -1, indicating no specific character class
- The function includes an assertion to ensure the input pointer is not NULL
- Returns the same pointer for convenience in function chaining
- Part of PostgreSQL's regular expression compilation subsystem