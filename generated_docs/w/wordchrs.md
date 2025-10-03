# wordchrs

## Location
[src/backend/regex/regcomp.c:1993-2028](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regcomp.c#L1993-L2028)

## Overview
Sets up a cached word-character list for word-boundary processing by creating circular arcs on a dedicated state to represent all characters that match the \w character class.

## Definition

```c
static void
wordchrs(struct vars *v)
```
## Detailed Description
The wordchrs function creates and caches a representation of word characters (\w character class) for efficient word-boundary matching in regular expressions. It constructs a dedicated state with circular arcs that represent all characters considered to be word characters according to the current locale and case-sensitivity settings.

The function implements a lazy initialization pattern - it only builds the word character cache when first needed and reuses it for subsequent operations. The cache is stored as a set of circular arcs on an otherwise-unused state, allowing for efficient lookup during regex matching.

The function performs several important steps:
1. Checks if the cache already exists to avoid redundant work
2. Creates a dummy state to hold the cache arcs
3. Obtains the character vector for word characters based on locale and case settings
4. Builds arcs representing these characters, which may trigger color splitting
5. Closes any new subcolors to ensure the cache is self-contained

## Parameters / Member Variables
- `*v`: Pointer to the vars structure containing regex compilation state, including the wordchrs cache pointer and compilation flags
## Dependencies
- Functions called/Symbols referenced:
  - [newstate](../n/newstate.md) (creates a new NFA state)
  - [cclasscvec](../c/cclasscvec.md) (gets character vector for character class)
  - [subcolorcvec](../s/subcolorcvec.md) (creates arcs for character vector)
  - [okcolors](../o/okcolors.md) (closes open subcolors)
  - NOERR (error checking macro)
  - NOTE (debugging/logging macro)
- Data structures used:
  - [cvec](../c/cvec.md) (character vector structure)
  - [state](../s/state.md) (NFA state structure)
- Constants used:
  - [CC_WORD](../C/CC_WORD.md) (word character class identifier)
  - REG_ICASE, REG_ULOCALE (regex compilation flags)
- Called from (representative examples):
  - [word](word.md) function (regcomp.c:1482)
  - [nonword](../n/nonword.md) function (regcomp.c:1468)
  - ARCV macro usage in various locations

## Notes and Other Information
- This function must not be called while there are open subcolors, as it would interfere with color bookkeeping
- The limitation on open subcolors prevents similar optimization in charclass and complement functions within bracket expressions
- The cache is stored in v->wordchrs and persists for the duration of the regex compilation
- Uses locale-aware character classification and respects case-insensitive matching flags
- The circular arcs pattern allows for efficient character matching during regex execution
- Color splitting may occur during arc construction to maintain proper color organization

## Simplified Source

```c
static void
wordchrs(struct vars *v)
{
    struct state *cstate;
    struct cvec *cv;

    // Skip if already cached
    if (v->wordchrs != NULL)
        return;

    // Create dummy state to hold cache arcs
    cstate = newstate(v->nfa);
    NOERR();

    // Get character vector for \w characters (word chars)
    NOTE(REG_ULOCALE);
    cv = cclasscvec(v, CC_WORD, (v->cflags & REG_ICASE));
    NOERR();

    // Build arcs representing word characters
    subcolorcvec(v, cv, cstate, cstate);
    NOERR();

    // Close any new subcolors to make cache self-contained
    okcolors(v->nfa, v->cm);
    NOERR();

    // Cache the result for future use
    v->wordchrs = cstate;
}
```