# SplitToVariants

## Location
[src/backend/tsearch/spell.c:2374-2523](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/spell.c#L2374-L2523)

## Overview
A recursive function that splits compound words into component parts and generates all possible variants for spell checking in PostgreSQL's text search functionality.

## Definition
```c
static SplitVar *SplitToVariants(IspellDict *Conf, SPNode *snode, SplitVar *orig, char *word, int wordlen, int startpos, int minpos)
```

## Detailed Description
SplitToVariants is a complex recursive function that analyzes compound words by attempting to split them at various positions and checking if the resulting parts exist in the dictionary. It traverses the spell dictionary trie structure while tracking possible word boundaries, handling compound affixes, and generating multiple splitting variants. The function implements PostgreSQL's compound word recognition algorithm, which is essential for languages that frequently use compound words. It uses a backtracking approach to explore all possible valid splits and builds a linked list of SplitVar structures containing the different word stem combinations.

## Parameters / Member Variables
- `Conf`: IspellDict configuration containing dictionary and affix rules
- `snode`: Current node in the spell dictionary trie (NULL to start from root)
- `orig`: Original SplitVar structure to copy and extend
- `word`: Input word string to be split
- `wordlen`: Length of the input word
- `startpos`: Starting position for current split attempt
- `minpos`: Minimum position for valid word boundaries

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth (stack overflow protection)
  - CopyVar (copying SplitVar structures)
  - CheckCompoundAffixes (compound affix validation)
  - NormalizeSubWord (word normalization)
  - AddStem (adding stems to variants)
  - palloc/pfree (memory management)
  - pnstrdup (string duplication)
- Called from (representative examples):
  - SplitToVariants (recursive calls at src/backend/tsearch/spell.c:2446, 2501)
  - NINormalizeWord (at src/backend/tsearch/spell.c:2565)

## Notes and Other Information
- Implements recursive backtracking with stack depth checking to prevent overflow
- Handles three types of compound positions: FF_COMPOUNDBEGIN, FF_COMPOUNDMIDDLE, FF_COMPOUNDLAST
- Uses a 'notprobed' array to avoid redundant checks at the same positions
- Performs binary search on trie nodes for efficient character matching
- The function can generate multiple splitting variants for the same word
- Critical for text search in languages with extensive compound word usage
- Part of PostgreSQL's Ispell-based spell checking infrastructure