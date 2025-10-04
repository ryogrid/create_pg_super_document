# newlacon

## Location
[src/backend/regex/regcomp.c:2391-2429](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regcomp.c#L2391-L2429)

## Overview
Allocates and initializes a new lookaround-constraint sub-regular expression (lacon) structure for handling lookahead and lookbehind assertions in regex patterns.

## Definition

```c
static int						/* lacon number */
newlacon(struct vars *v,
		 struct state *begin,
		 struct state *end,
		 int latype)
```
## Detailed Description
The newlacon function is responsible for managing the dynamic allocation and initialization of lookaround constraint structures (lacons) in PostgreSQL's regex engine. Lookaround assertions include lookahead (?=...), negative lookahead (?!...), lookbehind (?<=...), and negative lookbehind (?<!...) patterns.

The function maintains a dynamically growing array of lacon structures within the vars context. When called, it either allocates the initial array (skipping index 0 for special purposes) or expands the existing array to accommodate a new lacon entry. Each lacon represents a specific lookaround constraint with begin/end states and a type indicator.

Key operations performed:
1. Determines the next available lacon index (starting from 1, skipping 0)
2. Allocates or reallocates the lacons array to accommodate the new entry
3. Initializes the new lacon with the provided begin/end states and type
4. Clears the compiled NFA structure (cnfa) for the new lacon
5. Returns the assigned lacon number for reference

## Parameters / Member Variables
- `*v`: vars structure containing the regex compilation context and lacon array
- `*begin`: state pointer marking the beginning of the lookaround constraint
- `*end`: state pointer marking the end of the lookaround constraint
- `latype`: integer indicating the type of lookaround (ahead/behind, positive/negative)
## Dependencies
- Functions called/Symbols referenced:
  - MALLOC - Allocates initial memory for lacons array
  - REALLOC - Expands existing lacons array
  - ERR - Sets error condition
  - REG_ESPACE - Out of space error code
  - ZAPCNFA - Clears compiled NFA structure
- Called from (representative examples):
  - [processlacon](../p/processlacon.md) - Main lacon processing function

## Notes and Other Information
- Returns the allocated lacon number (starting from 1) or 0 on error
- Index 0 in the lacons array is intentionally skipped for special purposes
- Memory allocation failures result in REG_ESPACE error
- The cnfa (compiled NFA) field is zeroed for new lacons
- Part of PostgreSQL's advanced regex features supporting complex lookaround assertions
- Dynamic array management ensures efficient memory usage for varying numbers of lookaround constraints

## Simplified Source

```c
static int newlacon(struct vars *v, struct state *begin, struct state *end, int latype) {
    int n;
    struct subre *newlacons;
    struct subre *sub;

    // Determine next lacon index (start from 1, skip 0)
    if (v->nlacons == 0) {
        n = 1;
        newlacons = (struct subre *) MALLOC(2 * sizeof(struct subre));
    } else {
        n = v->nlacons;
        newlacons = (struct subre *) REALLOC(v->lacons, (n + 1) * sizeof(struct subre));
    }

    // Handle allocation failure
    if (newlacons == NULL) {
        ERR(REG_ESPACE);
        return 0;
    }

    // Update array and initialize new lacon
    v->lacons = newlacons;
    v->nlacons = n + 1;
    sub = &v->lacons[n];
    sub->begin = begin;
    sub->end = end;
    sub->latype = latype;
    ZAPCNFA(sub->cnfa);

    return n;
}
```