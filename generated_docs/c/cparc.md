# cparc

## Location
src/backend/regex/regc_nfa.c: 608 - 619

## Overview
Creates a new arc in an NFA by copying the type and color properties from an existing arc but with new source and destination states.

## Definition
```c
static void cparc(struct nfa *nfa, struct arc *oa, struct state *from, struct state *to)
```

## Detailed Description
The `cparc` function is a convenience wrapper around the `newarc` function that creates a new arc by copying attributes from an existing arc. It extracts the type and color from the old arc (`oa`) and creates a new arc with those same properties but connecting different states. This function is commonly used during NFA transformations where the same type of transition needs to be replicated between different states.

The function name "cparc" likely stands for "copy arc". It's a simple but frequently used operation in NFA construction and optimization, allowing code to duplicate arc properties without having to explicitly extract and pass the type and color parameters.

## Parameters / Member Variables
- `nfa`: The NFA structure where the new arc will be created
- `oa`: The old arc whose type and color properties should be copied
- `from`: The source state for the new arc
- `to`: The destination state for the new arc

## Dependencies
- Functions called/Symbols referenced:
  - struct arc (data structure)
  - struct nfa (data structure)
  - struct state (data structure)
  - newarc (function to create new arcs)
- Called from (representative examples):
  - moveins (in regc_nfa.c:802)
  - copyins (in regc_nfa.c:904)
  - moveouts (in regc_nfa.c:1090)
  - copyouts (in regc_nfa.c:1189)
  - duptraverse (in regc_nfa.c:1408)
  - pull (in regc_nfa.c:1750, 1786, 1787)
  - push (in regc_nfa.c:1921, 1957, 1958)
  - breakconstraintloop (in regc_nfa.c:2659)
  - clonesuccessorstates (in regc_nfa.c:2868, 2886, 2895)
  - makesearch (in regcomp.c:700)

## Notes and Other Information
- This is a static function internal to the regex NFA construction module
- Serves as a convenience wrapper to simplify arc copying operations
- Widely used throughout the NFA manipulation and optimization code
- Part of PostgreSQL's internal regular expression engine implementation
- The function delegates all the actual work to `newarc`, simply providing a more convenient interface for copying arc properties