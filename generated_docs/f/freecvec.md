# freecvec

## Location
[src/backend/regex/regc_cvec.c:135-138](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_cvec.c#L135-L138)

## Overview
Deallocates memory occupied by a character vector (cvec) structure, completely freeing the data structure from memory.

## Definition
```c
static void freecvec(struct cvec *cv)
```

## Detailed Description
The `freecvec` function is a memory management utility in PostgreSQL's regular expression engine that deallocates a character vector structure (`cvec`). This function performs a complete cleanup of the `cvec` structure, freeing all associated memory including the structure itself and any dynamically allocated character and range arrays that were allocated as part of the structure.

The function is designed to work with `cvec` structures that were allocated by `newcvec()`, where the character and range arrays are allocated as part of a single memory block along with the structure itself (as documented in the cvec structure comments). This makes the deallocation simple - a single `FREE()` call releases all associated memory.

## Parameters / Member Variables
- `cv`: Pointer to the `cvec` structure to be freed. This should be a valid pointer to a `cvec` that was allocated by the regex subsystem.

## Dependencies
- Functions called/Symbols referenced:
  - [cvec](../c/cvec.md) (structure type)
  - `FREE` (memory deallocation macro)
- Called from (representative examples):
  - `getcvec` (src/backend/regex/regc_cvec.c:123)
  - `REPLACEARC` (src/backend/regex/regcomp.c:249)
  - [freev](freev.md) (src/backend/regex/regcomp.c:606, 608)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the same source file (regc_cvec.c)
- The function uses the `FREE()` macro rather than direct `free()` call, which is part of PostgreSQL's memory management system
- Should only be called on `cvec` structures that were allocated by `newcvec()` or similar allocation functions
- No NULL pointer checking is performed - the caller is responsible for ensuring valid input
- The function is designed to work with the specific memory layout where chrs[] and ranges[] arrays are allocated together with the struct
- Part of PostgreSQL's regular expression compilation subsystem for character class management
- Once called, the `cvec` pointer becomes invalid and should not be used again

## Simplified Source

```c
static void
freecvec(struct cvec *cv)
{
    // Free the entire cvec structure and its embedded arrays
    FREE(cv);
}
```