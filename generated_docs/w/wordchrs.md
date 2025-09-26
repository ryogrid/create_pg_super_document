# wordchrs

## Location
src/backend/regex/regcomp.c: 1993 - 2028

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
- : Pointer to the vars structure containing regex compilation state, including the wordchrs cache pointer and compilation flags

## Dependencies
- Functions called/Symbols referenced:
  - newstate (creates a new NFA state)
  - cclasscvec (gets character vector for character class)
  - subcolorcvec (creates arcs for character vector)
  - okcolors (closes open subcolors)
  - NOERR (error checking macro)
  - NOTE (debugging/logging macro)
- Data structures used:
  - cvec (character vector structure)
  - state (NFA state structure)
- Constants used:
  - CC_WORD (word character class identifier)
  - REG_ICASE, REG_ULOCALE (regex compilation flags)
- Called from (representative examples):
  - word function (regcomp.c:1482)
  - nonword function (regcomp.c:1468)
  - ARCV macro usage in various locations

## Notes and Other Information
- This function must not be called while there are open subcolors, as it would interfere with color bookkeeping
- The limitation on open subcolors prevents similar optimization in charclass and complement functions within bracket expressions
- The cache is stored in v->wordchrs and persists for the duration of the regex compilation
- Uses locale-aware character classification and respects case-insensitive matching flags
- The circular arcs pattern allows for efficient character matching during regex execution
- Color splitting may occur during arc construction to maintain proper color organization