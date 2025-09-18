# mkANode

## Location
src/backend/tsearch/spell.c: 1830 - 1906

## Overview
mkANode builds a prefix tree (Trie) for affix rules with non-empty replacement strings, creating an efficient hierarchical structure for affix matching during spell checking.

## Definition


## Detailed Description
This recursive function constructs a prefix tree from a range of affix rules to enable fast pattern matching. It processes affixes level by level, grouping them by their character at the current position. The function builds a tree where:

1. **Character Counting**: First pass counts unique characters at the current level
2. **Node Creation**: Allocates an AffixNode with appropriate size for character branches  
3. **Recursive Building**: For each unique character, recursively builds child nodes for the next level
4. **Affix Collection**: Collects complete affixes (where replacement length equals current level + 1)

The resulting tree structure allows efficient traversal during affix matching, where each node represents a character position and contains both child nodes for longer patterns and completed affix rules.

## Parameters / Member Variables
- : Pointer to IspellDict containing affix configuration and data
- : Lower index in the Conf->Affix array for processing range
- : Upper index in the Conf->Affix array for processing range  
- : Current depth/level in the prefix tree being built
- : Affix type - either FF_SUFFIX or FF_PREFIX indicating processing direction

## Dependencies
- Functions called/Symbols referenced:
  - GETCHAR (macro for character extraction)
  - tmpalloc
  - cpalloc0
  - cpalloc
  - memcpy
  - [pfree](../p/pfree.md)
  - [mkANode](mkANode.md) (recursive self-call)
- Called from (representative examples):
  - [mkANode](mkANode.md) (recursive calls)
  - [NISortAffixes](../N/NISortAffixes.md)

## Notes and Other Information
- Only processes affixes with non-empty replacement strings; empty affixes are handled by mkVoidAffix()
- Uses recursive strategy to build tree level by level
- Memory allocation uses cpalloc0 for the main node and cpalloc for affix arrays
- The ANHRDSZ constant defines the header size for AffixNode structures
- Temporary affix array is allocated with tmpalloc and freed after use
- Returns NULL if no characters are found at the current level, indicating end of tree branch