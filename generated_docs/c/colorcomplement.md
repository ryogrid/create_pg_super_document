# colorcomplement

## Location
[src/backend/regex/regc_color.c:1064-1126](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_color.c#L1064-L1126)

## Overview
Creates arcs for all complementary colors that are not matched by the output arcs of a given state, used in regular expression character class negation.

## Definition

```c
static void
colorcomplement(struct nfa *nfa,
				struct colormap *cm,
				int type,
				struct state *of,
				struct state *from,
				struct state *to)
```
## Detailed Description
The colorcomplement function implements character class negation in regular expressions by creating arcs for all colors (character classes) that are NOT matched by the specified state's output arcs. This is essential for implementing constructs like [^abc] which matches any character except 'a', 'b', or 'c'.

The function works in three phases:
1. **RAINBOW detection**: If the reference state has a RAINBOW arc (which matches all colors), the complement would be empty, so it creates a CANTMATCH arc instead to maintain NFA connectivity
2. **Marking phase**: Temporarily marks all colors that appear in the reference state's PLAIN output arcs
3. **Complement creation**: Creates new arcs for all unmarked colors (excluding pseudocolors and unused colors)

The function ensures NFA connectivity is maintained even when the complement set is empty, which is crucial for proper regex engine operation.

## Parameters / Member Variables
- : The nondeterministic finite automaton being constructed
- : The color map that defines character-to-color mappings
- : The type of arc to create for complement colors
- : The reference state whose output arcs define what to complement
- : The source state for new complement arcs
- : The destination state for new complement arcs

## Dependencies
- Functions called/Symbols referenced:
  - [findarc](../f/findarc.md) (to check for RAINBOW arcs)
  - [newarc](../n/newarc.md) (to create new arcs)
  - [CDEND](../C/CDEND.md) (color descriptor end marker)
  - UNUSEDCOLOR (macro to check if color is unused)
  - CISERR (macro to check for compilation errors)
- Called from (representative examples):
  - [nonword](../n/nonword.md) (src/backend/regex/regcomp.c:1468)
  - [charclasscomplement](charclasscomplement.md) (src/backend/regex/regcomp.c:1544)
  - [cbracket](cbracket.md) (src/backend/regex/regcomp.c:1752)
  - processlacon (src/backend/regex/regcomp.c:2058, 2076)

## Notes and Other Information
- The function includes special handling for RAINBOW arcs to prevent NFA disconnection
- Uses transient COLMARK flags to track which colors should be excluded from the complement
- Essential for implementing negated character classes in regular expressions
- The calling sequence is noted to need reconciliation with cloneouts() function
- Sets HASCANTMATCH flag when creating CANTMATCH arcs for later cleanup during NFA optimization