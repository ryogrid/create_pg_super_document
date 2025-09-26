# RegisNode

## Location
src/include/tsearch/dicts/regis.h: 17 - 25

## Overview
RegisNode is a compact data structure that represents a node in a fast regular expression subset engine used by the ISpell dictionary implementation in PostgreSQL's text search functionality.

## Definition


## Detailed Description
RegisNode is the fundamental building block for PostgreSQL's fast regex subset implementation used by ISpell dictionaries. This structure is designed to be compact and efficient, using bitfields to pack multiple pieces of information into a single 32-bit word. The node represents a single element in a linked list of regex pattern components, where each node can contain character data and type information for pattern matching operations. The design prioritizes memory efficiency while providing the necessary functionality for fast pattern matching in text search operations.

## Parameters / Member Variables
- : 2-bit field indicating the node type (likely RSF_ONEOF=1 or RSF_NONEOF=2 based on constants)
- : 16-bit field storing the length of data contained in this node
- : 14-bit padding field for future use or alignment
- : Pointer to the next RegisNode in the linked list structure
- : Flexible array member containing the actual character data for pattern matching

## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER (for variable-length data storage)
- Called from (representative examples):
  - RS_isRegis (regex pattern detection)
  - newRegisNode (node creation)
  - RS_compile (pattern compilation)
  - RS_free (memory cleanup)
  - RS_execute (pattern matching execution)
  - Regis (parent structure that contains RegisNode)

## Notes and Other Information
- The structure uses bitfields for memory efficiency, packing type and length information into a single 32-bit word
- RNHDRSZ macro (defined as offsetof(RegisNode,data)) provides the header size excluding the flexible array member
- The flexible array member allows variable-length data storage while maintaining type safety
- This is part of PostgreSQL's text search infrastructure, specifically designed for ISpell dictionary pattern matching
- The design reflects a trade-off between memory efficiency and computational speed for regex operations