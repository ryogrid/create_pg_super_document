# mkSPNode

## Location
[src/backend/tsearch/spell.c:1639-1720](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/spell.c#L1639-L1720)

## Overview
Recursively constructs a prefix tree (trie) structure for spell-checking, organizing dictionary words by character prefixes at each level.

## Definition
```c
static SPNode *mkSPNode(IspellDict *Conf, int low, int high, int level)
```

## Detailed Description
This function builds a prefix tree structure for efficient spell-checking by recursively partitioning the sorted dictionary words. At each level, it groups words by their character at the current position, creating SPNode structures that contain character values and either child nodes (for continued paths) or word completion information (for word endings). The function handles affix merging when multiple words share the same prefix but have different affixes, and manages compound word flags appropriately. It implements special logic for compound-only words and ensures proper flag inheritance.

## Parameters / Member Variables
- `Conf`: Pointer to IspellDict structure containing the sorted dictionary
- `low`: Lower index boundary in the Conf->Spell array for current partition
- `high`: Upper index boundary in the Conf->Spell array for current partition  
- `level`: Current depth/character position in the prefix tree

## Dependencies
- Functions called/Symbols referenced:
  - cpalloc0 (zero-initialized memory allocation)
  - [mkSPNode](mkSPNode.md) (recursive calls for child nodes)
  - [makeCompoundFlags](makeCompoundFlags.md) (extracts compound flags from affix)
  - [MergeAffix](../M/MergeAffix.md) (merges multiple affix sets)
  - SPNHDRSZ (SPNode header size constant)
  - FF_COMPOUNDONLY/FF_COMPOUNDFLAG (compound word flag constants)
- Called from (representative examples):
  - [mkSPNode](mkSPNode.md) (recursive self-calls)
  - [NISortDictionary](../N/NISortDictionary.md) (initial tree construction)

## Notes and Other Information
- Returns NULL if no characters are found at the current level
- Allocates SPNode with header plus array of SPNodeData for each unique character
- Handles word completion by setting isword flag and storing affix information
- Implements affix merging logic when multiple words end at the same node
- Manages compound word flags with special handling for FF_COMPOUNDONLY
- Automatically promotes compound-only words to compound flags when appropriate
- Uses clearCompoundOnly logic to handle conflicting compound permissions
- Tree structure enables efficient prefix-based word lookup and validation