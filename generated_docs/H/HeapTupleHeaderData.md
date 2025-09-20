# HeapTupleHeaderData

## Location
[src/include/access/htup_details.h:153-165](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/htup_details.h#L153-L165)

## Overview
HeapTupleHeaderData is the fundamental header structure for heap tuples in PostgreSQL, containing all metadata necessary for transaction visibility, tuple identification, and data layout information.

## Definition

```c
struct HeapTupleHeaderData
{
	union
	{
		HeapTupleFields t_heap;
		DatumTupleFields t_datum;
	}			t_choice;

	ItemPointerData t_ctid;		/* current TID of this or newer tuple (or a
								 * speculative insertion token) */

	/* Fields below here must match MinimalTupleData! */

#define FIELDNO_HEAPTUPLEHEADERDATA_INFOMASK2 2
	uint16		t_infomask2;	/* number of attributes + various flags */

#define FIELDNO_HEAPTUPLEHEADERDATA_INFOMASK 3
	uint16		t_infomask;		/* various flag bits, see below */

#define FIELDNO_HEAPTUPLEHEADERDATA_HOFF 4
	uint8		t_hoff;			/* sizeof header incl. bitmap, padding */

	/* ^ - 23 bytes - ^ */

#define FIELDNO_HEAPTUPLEHEADERDATA_BITS 5
	bits8		t_bits[FLEXIBLE_ARRAY_MEMBER];	/* bitmap of NULLs */

	/* MORE DATA FOLLOWS AT END OF STRUCT */
};
```
## Detailed Description
HeapTupleHeaderData serves as the complete header structure for tuples stored in PostgreSQL heap files. It contains all the metadata required for MVCC (Multi-Version Concurrency Control), transaction visibility determination, tuple versioning, and data layout management.

The structure uses a union to support two different operational modes: heap tuples (normal table storage) and datum tuples (composite type values). The header includes transaction identifiers for MVCC, a tuple identifier for version chaining, various flag bits for tuple properties, and a flexible bitmap for tracking NULL values.

Key design considerations include support for speculative insertion tokens, tuple version chaining through t_ctid links, and compatibility with MinimalTupleData for certain operations.

## Parameters / Member Variables
- : Union containing either HeapTupleFields (for heap tuples) or DatumTupleFields (for datum tuples)
  - : Contains transaction IDs (t_xmin, t_xmax) and command ID information for heap tuples
  - : Contains type information for composite datum values
- : ItemPointerData pointing to current TID of this or newer tuple version, or speculative insertion token
- : 16-bit field containing number of attributes plus various flags
- : 16-bit field with various flag bits indicating tuple properties (null values, variable width attributes, external storage, locking information, etc.)
- : 8-bit field indicating size of header including bitmap and padding
- : Flexible array member containing bitmap of NULL values (only present when tuple has NULLs)

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleFields
  - [DatumTupleFields](../D/DatumTupleFields.md)
  - [ItemPointerData](../I/ItemPointerData.md)
- Called from (representative examples):
  - [expand_tuple](../e/expand_tuple.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [heap_tuple_from_minimal_tuple](../h/heap_tuple_from_minimal_tuple.md)
  - [heap_xlog_insert](../h/heap_xlog_insert.md)
  - [heap_xlog_multi_insert](../h/heap_xlog_multi_insert.md)
  - [heap_xlog_update](../h/heap_xlog_update.md)

## Notes and Other Information
- The structure supports PostgreSQL's MVCC system by storing virtual fields Xmin, Cmin, Xmax, Cmax, and Xvac in optimized physical storage
- Transaction visibility is determined through the transaction ID fields in the HeapTupleFields union member
- The t_ctid field enables tuple version chaining - following the chain leads to the newest version of a row
- Speculative insertion tokens can be stored in t_ctid during uncertain insertions
- Fields from t_infomask2 onward must match MinimalTupleData structure for compatibility
- The structure size is 23 bytes plus the variable-length NULL bitmap
- Located in src/include/access/htup_details.h:153-181 with typedef in src/include/access/htup.h:21