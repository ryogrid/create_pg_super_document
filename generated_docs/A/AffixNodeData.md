# AffixNodeData

## Location
[src/include/tsearch/dicts/spell.h:136-137](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/tsearch/dicts/spell.h#L136-L137)

## Overview
AffixNodeData is a structure that represents data stored in nodes of a prefix tree (Trie) used to efficiently store and organize affix lists in PostgreSQL's ISpell dictionary system.

## Definition

```c
typedef struct AffixNode
{
	uint32		isvoid:1,
				length:31;
	AffixNodeData data[FLEXIBLE_ARRAY_MEMBER];
} AffixNode;
```
## Detailed Description
AffixNodeData serves as the data component of nodes in a prefix tree (Trie) structure used to organize affix rules in ISpell dictionaries. Each node contains character value information, the number of affixes associated with that node, pointers to the actual affix data, and references to child nodes in the tree. This tree structure enables efficient prefix-based lookups of affix rules during word normalization and spell checking operations.

The structure uses bit fields to pack the character value and affix count into a single 32-bit integer, optimizing memory usage. The  field stores an 8-bit character value, while  stores the count of affixes in a 24-bit field, allowing for up to 16 million affixes per node (though practical limits are much lower).

## Parameters / Member Variables
- `val`: 8-bit character value representing the character at this node in the trie
- `naff`: 24-bit count indicating the number of affixes stored at this node
- `aff`: Pointer to an array of AFFIX pointers containing the actual affix data
- `node`: Pointer to child AffixNode, enabling tree traversal

## Dependencies
- Functions called/Symbols referenced:
  - AFFIX (affix structure type)
  - [AffixNode](AffixNode.md) (parent node structure)
- Called from (representative examples):
  - [mkANode](../m/mkANode.md) (creates affix nodes)
  - [mkVoidAffix](../m/mkVoidAffix.md) (creates empty affix nodes)  
  - [NISortAffixes](../N/NISortAffixes.md) (sorts affix data)
  - [FindAffixes](../F/FindAffixes.md) (searches for matching affixes)
  - [NormalizeSubWord](../N/NormalizeSubWord.md) (uses affixes for word normalization)

## Notes and Other Information
- Part of the AffixNode structure which uses this as its data array
- Optimized memory layout using bit fields for efficient storage
- Enables fast prefix-based searches in affix trees
- Critical component of PostgreSQL's text search spell checking functionality
- Works in conjunction with SPNode/SPNodeData for complete dictionary functionality