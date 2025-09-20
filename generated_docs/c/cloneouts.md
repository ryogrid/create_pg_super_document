# cloneouts

## Location
[src/backend/regex/regc_nfa.c:1256-1280](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_nfa.c#L1256-L1280)

## Overview
Copies outgoing arcs from one state to another state pair while modifying the arc type from PLAIN to AHEAD or BEHIND.

## Definition

```c
static void
cloneouts(struct nfa *nfa,
		  struct state *old,
		  struct state *from,
		  struct state *to,
		  int type)
```
## Detailed Description
The `cloneouts` function is a specialized arc copying utility specifically designed for converting PLAIN arcs to lookahead/lookbehind arcs (AHEAD/BEHIND types). It creates new arcs between a specified from-to state pair, copying the color information from the original arcs but changing their type. This function is primarily used in the context of processing lookahead and lookbehind assertions in regular expressions, where the same pattern matching logic needs to be applied with different semantic meaning.

The function ensures type safety by asserting that all source arcs are PLAIN type and the target type is either AHEAD or BEHIND. It would not be appropriate for use with LACON (lookaround constraint) arcs due to different interpretation requirements.

## Parameters / Member Variables
- `nfa`: Pointer to the NFA structure being modified
- `old`: Source state whose outgoing arcs will be cloned (must be different from 'from' state)
- `from`: Source state for the new arcs to be created
- `to`: Destination state for the new arcs to be created  
- `type`: Type for the new arcs (must be AHEAD or BEHIND)

## Dependencies
- Functions called/Symbols referenced:
  - [newarc](../n/newarc.md)
  - AHEAD
  - BEHIND  
  - PLAIN
- Called from (representative examples):
  - [word](../w/word.md)
  - processlacon

## Notes and Other Information
- Only works with PLAIN to AHEAD/BEHIND type conversion (assertion checks enforce this)
- Preserves the color (co) information from original arcs
- Not suitable for LACON arcs due to different constraint interpretation
- Used specifically for lookahead/lookbehind assertion processing
- Part of the regex compilation system for handling advanced regex features
- Located in src/backend/regex/regc_nfa.c:1256-1280