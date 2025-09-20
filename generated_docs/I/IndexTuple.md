# IndexTuple

## Location
[src/include/access/itup.h:53-54](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/itup.h#L53-L54)

## Overview
IndexTuple is a typedef that represents a pointer to an IndexTupleData structure, which is the fundamental data structure used to represent index entries in PostgreSQL's indexing system.

## Definition

```c
typedef IndexTupleData *IndexTuple;
```
## Detailed Description
IndexTuple is a pointer type that references IndexTupleData structures. The IndexTupleData structure serves as the header for all index tuples in PostgreSQL. It contains essential metadata about the index entry including a reference to the corresponding heap tuple and various flags indicating the tuple's characteristics. The actual index attribute values follow this header structure in memory, beginning at a MAXALIGN boundary. If the tuple has null values, an IndexAttributeBitMapData structure is placed between the header and the attribute values.

## Parameters / Member Variables
Since IndexTuple is a typedef for a pointer to IndexTupleData, the relevant members are those of IndexTupleData:

-  (ItemPointerData): Reference TID (Tuple Identifier) that points to the corresponding heap tuple
-  (unsigned short): Packed information field containing various flags and metadata:
  - Bit 15 (high bit): Indicates if the tuple has null values
  - Bit 14: Indicates if the tuple has variable-width attributes
  - Bit 13: Access Method-defined meaning (interpretation depends on the specific index AM)
  - Bits 12-0: Size of the tuple in bytes

## Dependencies
- Functions called/Symbols referenced:
  - [IndexTupleData](IndexTupleData.md) (the underlying structure)
  - [ItemPointerData](ItemPointerData.md) (for t_tid field)

- Called from (representative examples):
  - Various index access method implementations
  - Index scanning and insertion routines
  - Index tuple manipulation functions throughout the PostgreSQL codebase

## Notes and Other Information
- The IndexTuple typedef provides a convenient pointer abstraction for working with index tuples
- The underlying IndexTupleData structure is designed to minimize space usage since index tuples are stored frequently
- The structure comment indicates "MORE DATA FOLLOWS AT END OF STRUCT", meaning the actual indexed attribute values are stored immediately after the fixed header
- The t_info field uses bit packing to store multiple pieces of information in a single 16-bit field to save space
- Index tuples are used across all index types in PostgreSQL (B-tree, Hash, GiST, GIN, SP-GiST, BRIN)
- The design allows for efficient storage and retrieval of index entries while maintaining references back to the heap tuples they index