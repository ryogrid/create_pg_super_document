# charclass

## Location
[src/backend/regex/regcomp.c:1494-1517](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regcomp.c#L1494-L1517)

## Overview
The charclass function generates NFA arcs for character class matching in regular expressions, supporting both predefined character classes and bracket expression elements.

## Definition
```c
static void
charclass(struct vars *v,
          enum char_classes cls,
          struct state *lp,
          struct state *rp)
```

## Detailed Description
The charclass function is part of PostgreSQL's regular expression engine implementation. It creates arcs in the NFA (Non-deterministic Finite Automaton) for matching character classes such as \w (word characters), \d (digits), \s (whitespace), etc.

The function works by:
1. Obtaining a cached character vector (cvec) for the specified character class
2. Handling case-insensitive matching when the REG_ICASE flag is set
3. Building the appropriate arcs using subcolorcvec, which may cause color splitting for optimization

This function is used both for atom-level character classes (like \w) and for elements within bracket expressions (like [[:alpha:]]).

## Parameters / Member Variables
- `v`: Pointer to vars structure containing regex compilation context and state
- `cls`: Enumerated character class type (e.g., CC_WORD, CC_DIGIT, CC_SPACE)
- `lp`: Left/source state pointer for the NFA arc
- `rp`: Right/destination state pointer for the NFA arc

## Dependencies
- Functions called/Symbols referenced:
  - char_classes (enum type)
  - [cvec](cvec.md) (struct type)
  - NOTE (macro)
  - REG_ULOCALE (constant)
  - [cclasscvec](cclasscvec.md)
  - REG_ICASE (flag)
  - NOERR (macro)
  - [subcolorcvec](../s/subcolorcvec.md)
- Called from (representative examples):
  - ARCV (in regcomp.c)
  - [brackpart](../b/brackpart.md) (multiple call sites in regcomp.c)

## Notes and Other Information
- This is a static function internal to the regex compilation module
- The caller is responsible for calling okcolors() after processing the atom or bracket
- The function handles locale-aware character classification via REG_ULOCALE
- Case-insensitive matching is supported through the REG_ICASE flag
- The function may cause color splitting for NFA optimization
- Character vectors (cvec) are cached for performance