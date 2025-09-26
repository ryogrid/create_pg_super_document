# IndexTupleData

## Location
[src/include/access/itup.h:35-51](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/itup.h#L35-L51)

## Overview
IndexTupleData is the header structure for all index tuples in PostgreSQL, providing essential metadata including a reference to the corresponding heap tuple and various tuple attributes encoded in a compact information field.

## Definition

```c
typedef struct IndexTupleData
{
	ItemPointerData t_tid;		/* reference TID to heap tuple */

	/* ---------------
	 * t_info is laid out in the following fashion:
	 *
	 * 15th (high) bit: has nulls
	 * 14th bit: has var-width attributes
	 * 13th bit: AM-defined meaning
	 * 12-0 bit: size of tuple
	 * ---------------
	 */

	unsigned short t_info;		/* various info about tuple */

} IndexTupleData;
```
## Detailed Description
IndexTupleData serves as the fundamental header structure for all index tuples in PostgreSQL's indexing system. This structure is always present at the beginning of every index tuple, regardless of the index access method (btree, hash, GiST, GIN, etc.). The structure is designed to be compact yet informative, containing only the essential metadata needed to interpret the index tuple.

The structure is followed by additional data depending on the tuple's characteristics. If the HasNulls bit is set in t_info, an IndexAttributeBitMapData structure follows immediately after IndexTupleData. The actual index attribute values begin at a MAXALIGN boundary after any bitmap data.

The design philosophy emphasizes space efficiency while maintaining the necessary information for tuple interpretation. The t_info field uses bit packing to store multiple pieces of information in a single 16-bit value, including null presence indicators, variable-width attribute flags, access method-specific data, and the tuple size.

## Parameters / Member Variables
- `t_tid`: ItemPointerData structure containing the tuple identifier (TID) that references the corresponding tuple in the heap table. This provides the link between the index entry and the actual data row.
- `t_info`: 16-bit unsigned integer containing packed information about the tuple:
  - Bit 15 (high): HasNulls flag indicating whether the tuple contains null values
  - Bit 14: HasVarWidth flag indicating whether the tuple has variable-width attributes
  - Bit 13: Access method-defined meaning (AM-specific usage)
  - Bits 12-0: Size of the tuple in bytes (allows tuples up to 8KB)

## Dependencies
- Functions called/Symbols referenced:
  - [ItemPointerData](ItemPointerData.md) (for t_tid member)
  - MAXALIGN (for data alignment requirements)
- Called from (representative examples):
  - [index_form_tuple_context](../i/index_form_tuple_context.md) (src/backend/access/common/indextuple.c:178)
  - [nocache_index_getattr](../n/nocache_index_getattr.md) (src/backend/access/common/indextuple.c:273)
  - [index_deform_tuple](../i/index_deform_tuple.md) (src/backend/access/common/indextuple.c:463)
  - [_bt_pgaddtup](../b/_bt_pgaddtup.md) (src/backend/access/nbtree/nbtinsert.c:2636)
  - [_bt_sortaddtup](../b/_bt_sortaddtup.md) (src/backend/access/nbtree/nbtsort.c:720)
  - [gistjoinvector](../g/gistjoinvector.md) (src/backend/access/gist/gistutil.c:125)

## Notes and Other Information
- The comment "MORE DATA FOLLOWS AT END OF STRUCT" indicates that this is a variable-length structure with additional data appended after the fixed header.
- The bitmap space allocation is fixed regardless of the actual number of attributes to avoid storing attribute count information in the header, which saves space given MAXALIGN constraints.
- The structure supports tuples up to 8KB in size (using 13 bits for size information).
- This structure is fundamental to PostgreSQL's index implementation and is used across all index access methods.
- The t_info field's bit layout is carefully designed to pack maximum information into minimal space while maintaining efficient access patterns.