# charclasscomplement

## Location
src/backend/regex/regcomp.c: 1518 - 1554

## Overview
The charclasscomplement function generates NFA arcs for complemented character class matching in regular expressions, handling negated character classes like \W, \D, \S, and negated bracket expressions.

## Definition
```c
static void
charclasscomplement(struct vars *v,
                    enum char_classes cls,
                    struct state *lp,
                    struct state *rp)
```

## Detailed Description
The charclasscomplement function is part of PostgreSQL's regular expression engine implementation. It creates arcs in the NFA (Non-deterministic Finite Automaton) for matching the complement (negation) of character classes such as \W (non-word characters), \D (non-digits), \S (non-whitespace), etc.

The function works through a multi-step process:
1. Creates a temporary dummy state to hold intermediate arcs
2. Obtains the character vector (cvec) for the original character class
3. Builds arcs for the original character class on the dummy state
4. Cleans up any subcolors created during the process
5. Uses colorcomplement to create arcs for all characters NOT in the class
6. Cleans up the temporary dummy state

This approach allows efficient computation of character class complements while maintaining the NFA's color optimization system.

## Parameters / Member Variables
- `v`: Pointer to vars structure containing regex compilation context and state
- `cls`: Enumerated character class type to be complemented (e.g., CC_WORD for \W)
- `lp`: Left/source state pointer for the NFA arc
- `rp`: Right/destination state pointer for the NFA arc

## Dependencies
- Functions called/Symbols referenced:
  - char_classes (enum type)
  - cvec (struct type)
  - newstate
  - NOERR (macro)
  - NOTE (macro)
  - REG_ULOCALE (constant)
  - cclasscvec
  - REG_ICASE (flag)
  - subcolorcvec
  - okcolors
  - colorcomplement
  - PLAIN (constant)
  - dropstate
- Called from (representative examples):
  - ARCV (in regcomp.c)
  - bracket (in regcomp.c)

## Notes and Other Information
- This is a static function internal to the regex compilation module
- The function requires that there be no open subcolors when called in bracket expressions
- Case-insensitive matching is supported through the REG_ICASE flag
- The function handles locale-aware character classification via REG_ULOCALE
- Uses a temporary state strategy to efficiently compute complements
- Multiple error checking points ensure robust operation
- The colorcomplement function does the heavy lifting of creating complement arcs