# AffixNode

## Location
[src/include/tsearch/dicts/spell.h:138-143](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/tsearch/dicts/spell.h#L138-L143)

## Overview
AffixNode is a structure that represents a node in a prefix tree (Trie) used to store and efficiently lookup affix rules in PostgreSQL's Ispell dictionary system.

## Definition
```c
typedef struct AffixNode
{
    uint32          isvoid:1,
                    length:31;
    AffixNodeData   data[FLEXIBLE_ARRAY_MEMBER];
} AffixNode;
```

## Detailed Description
AffixNode serves as the fundamental building block for implementing a prefix tree data structure that organizes affix rules for efficient lookup during spell checking operations. The structure represents both regular affix nodes that contain character-based branching information and special "void" nodes that handle affixes with empty replacement strings. Each node contains a length field indicating the number of child branches and a flexible array of AffixNodeData elements that store character values, affix rule arrays, and pointers to child nodes.

The prefix tree implementation enables PostgreSQL's spell checker to quickly navigate through potential affix transformations by following character-by-character paths through the tree, making affix application both memory-efficient and fast during word normalization processes.

## Parameters / Member Variables
- `isvoid`: Single bit flag indicating whether this is a special void node for handling affixes with empty replacement strings
- `length`: 31-bit field specifying the number of AffixNodeData elements in the data array, representing child branches from this node
- `data`: Flexible array of AffixNodeData structures containing character values, affix rule references, and pointers to child nodes

## Dependencies
- Functions called/Symbols referenced:
  - AffixNodeData (struct for storing node data and affix references)
  - FLEXIBLE_ARRAY_MEMBER (macro for flexible array implementation)
- Called from (representative examples):
  - mkANode (creates and builds AffixNode tree structures)
  - FindAffixes (traverses AffixNode tree for affix lookup)
  - mkVoidAffix (creates special void AffixNode entries)
  - NormalizeSubWord (uses AffixNode for word normalization)
  - NISortDictionary (processes AffixNode structures during sorting)

## Notes and Other Information
- Part of PostgreSQL's Ispell dictionary implementation located in src/include/tsearch/dicts/spell.h:138-143
- The ANHRDSZ macro is defined as offsetof(AffixNode, data) to calculate header size for memory allocation
- Supports both prefix and suffix affix rules through the same tree structure
- Void nodes handle special case affixes that don't add characters but may change word properties
- Used in conjunction with binary search algorithms for efficient character-based navigation
- Memory allocated using PostgreSQL's compact allocation system for optimal performance