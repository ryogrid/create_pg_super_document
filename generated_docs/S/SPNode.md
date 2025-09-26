# SPNode

## Location
[src/include/tsearch/dicts/spell.h:50-54](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/tsearch/dicts/spell.h#L50-L54)

## Overview
SPNode is a structure that represents a node in a prefix tree (Trie) used to store word lists for PostgreSQL's text search spell checking functionality.

## Definition

```c
typedef struct SPNode
{
	uint32		length;
	SPNodeData	data[FLEXIBLE_ARRAY_MEMBER];
} SPNode;
```
## Detailed Description
SPNode serves as the fundamental building block for implementing a prefix tree data structure in PostgreSQL's spell checking dictionary system. Each node contains a length field indicating the number of child nodes and a flexible array of SPNodeData elements that store the actual character data and navigation information for the trie. This structure enables efficient storage and lookup of dictionary words by organizing them in a tree where each path from root to leaf represents a complete word.

The prefix tree implementation allows for memory-efficient storage of large dictionaries while providing fast prefix-based lookups, which is essential for spell checking and word suggestion functionality in PostgreSQL's full-text search capabilities.

## Parameters / Member Variables
- : Number of SPNodeData elements in the data array, representing the number of child branches from this node
- : Flexible array of SPNodeData structures containing character values, word flags, affix references, and pointers to child nodes

## Dependencies
- Functions called/Symbols referenced:
  - SPNodeData (struct for node data storage)
  - FLEXIBLE_ARRAY_MEMBER (macro for flexible array implementation)
- Called from (representative examples):
  - [mkSPNode](../m/mkSPNode.md) (creates and initializes SPNode structures)
  - [FindWord](../F/FindWord.md) (traverses SPNode tree for word lookup)
  - [SplitToVariants](SplitToVariants.md) (uses SPNode for word variant generation)
  - [makeCompoundFlags](../m/makeCompoundFlags.md) (processes compound word flags in SPNode)

## Notes and Other Information
- Part of PostgreSQL's Ispell dictionary implementation located in src/include/tsearch/dicts/spell.h:50-54
- The SPNHDRSZ macro is defined as offsetof(SPNode,data) to calculate the header size
- Used in conjunction with Hunspell-compatible affix processing for advanced spell checking features
- The flexible array member allows for efficient memory allocation based on the actual number of child nodes needed