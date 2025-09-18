# FindAffixes

## Location
[src/backend/tsearch/spell.c:2028-2070](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/spell.c#L2028-L2070)

## Overview
Traverses an affix tree (trie) to find matching affixes for a given word, using binary search to efficiently navigate through the tree structure.

## Definition


## Detailed Description
FindAffixes implements a depth-first traversal of an affix tree (trie) structure to locate affixes that match a given word. The function uses binary search within each node to efficiently find matching characters. It supports both prefix and suffix affix searching depending on the type parameter, using the GETWCHAR macro to extract characters in forward or reverse order.

The function handles void affixes (empty affixes) as a special case and continues traversing the tree until either a matching affix is found or no more matches are possible. The level parameter tracks the current position in the word being processed.

## Parameters / Member Variables
- : Root node of the affix tree to search through
- : Input word string to find affixes for
- : Length of the input word
- : Pointer to current character position being processed (modified during traversal)
- : Affix type (FF_PREFIX for prefix, FF_SUFFIX for suffix)

## Dependencies
- Functions called/Symbols referenced:
  - GETWCHAR (macro for character extraction)
- Called from (representative examples):
  - [NormalizeSubWord](../N/NormalizeSubWord.md) (3 times at lines 2212, 2236, 2254)

## Notes and Other Information
- Returns NULL if no matching affix is found
- Uses binary search for efficient character matching within nodes
- Supports both prefix (FF_PREFIX) and suffix (FF_SUFFIX) affix types
- The function modifies the level parameter to track traversal progress
- Part of PostgreSQL's text search spell checking functionality