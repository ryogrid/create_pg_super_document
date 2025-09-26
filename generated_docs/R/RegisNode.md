# RegisNode

## Location
[src/include/tsearch/dicts/regis.h:17-25](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/tsearch/dicts/regis.h#L17-L25)

## Overview
RegisNode is a compact data structure that represents a node in a fast regular expression subset engine used by the ISpell dictionary implementation in PostgreSQL's text search functionality.

## Definition

```c
typedef struct RegisNode
{
	uint32
				type:2,
				len:16,
				unused:14;
	struct RegisNode *next;
	unsigned char data[FLEXIBLE_ARRAY_MEMBER];
} RegisNode;
```
## Detailed Description
RegisNode is the fundamental building block for PostgreSQL's fast regex subset implementation used by ISpell dictionaries. This structure is designed to be compact and efficient, using bitfields to pack multiple pieces of information into a single 32-bit word. The node represents a single element in a linked list of regex pattern components, where each node can contain character data and type information for pattern matching operations. The design prioritizes memory efficiency while providing the necessary functionality for fast pattern matching in text search operations.

## Parameters / Member Variables
- `type`: 2-bit field indicating the node type (likely RSF_ONEOF=1 or RSF_NONEOF=2 based on constants)
- `len`: 16-bit field storing the length of data contained in this node
- `unused`: 14-bit padding field for future use or alignment
- `next`: Pointer to the next RegisNode in the linked list structure
- `data`: Flexible array member containing the actual character data for pattern matching

## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER (for variable-length data storage)
- Called from (representative examples):
  - [RS_isRegis](RS_isRegis.md) (regex pattern detection)
  - [newRegisNode](../n/newRegisNode.md) (node creation)
  - [RS_compile](RS_compile.md) (pattern compilation)
  - [RS_free](RS_free.md) (memory cleanup)
  - [RS_execute](RS_execute.md) (pattern matching execution)
  - [Regis](Regis.md) (parent structure that contains RegisNode)

## Notes and Other Information
- The structure uses bitfields for memory efficiency, packing type and length information into a single 32-bit word
- RNHDRSZ macro (defined as offsetof(RegisNode,data)) provides the header size excluding the flexible array member
- The flexible array member allows variable-length data storage while maintaining type safety
- This is part of PostgreSQL's text search infrastructure, specifically designed for ISpell dictionary pattern matching
- The design reflects a trade-off between memory efficiency and computational speed for regex operations