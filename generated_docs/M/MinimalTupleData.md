# MinimalTupleData

## Location
[src/include/access/htup_details.h:629-651](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/htup_details.h#L629-L651)

## Overview
MinimalTupleData is a streamlined tuple header structure used for temporary tuples and internal operations in PostgreSQL, providing essential tuple metadata with reduced storage overhead.

## Definition

```c
struct MinimalTupleData
{
	uint32		t_len;			/* actual length of minimal tuple */

	char		mt_padding[MINIMAL_TUPLE_PADDING];

	/* Fields below here must match HeapTupleHeaderData! */

	uint16		t_infomask2;	/* number of attributes + various flags */

	uint16		t_infomask;		/* various flag bits, see below */

	uint8		t_hoff;			/* sizeof header incl. bitmap, padding */

	/* ^ - 23 bytes - ^ */

	bits8		t_bits[FLEXIBLE_ARRAY_MEMBER];	/* bitmap of NULLs */

	/* MORE DATA FOLLOWS AT END OF STRUCT */
};
```
## Detailed Description
MinimalTupleData represents a space-efficient tuple header designed for temporary operations where full transaction visibility information is not required. Unlike HeapTupleHeaderData, it omits transaction IDs and other MVCC-related fields, making it suitable for intermediate results, sorting operations, and other scenarios where tuples don't need to participate in the full transaction visibility system.

The structure maintains compatibility with HeapTupleHeaderData from the t_infomask2 field onward, allowing many tuple processing functions to work with both types. The padding field ensures proper alignment across different architectures.

## Parameters / Member Variables
- : 32-bit field containing the actual total length of the minimal tuple including header and data
- : Character array providing padding to ensure proper alignment (size determined by MINIMAL_TUPLE_PADDING)
- : 16-bit field containing number of attributes plus various flags (matches HeapTupleHeaderData)
- : 16-bit field with various flag bits indicating tuple properties (matches HeapTupleHeaderData)
- : 8-bit field indicating size of header including bitmap and padding (matches HeapTupleHeaderData)
- : Flexible array member containing bitmap of NULL values (matches HeapTupleHeaderData)

## Dependencies
- Functions called/Symbols referenced: None directly
- Called from (representative examples):
  - [expand_tuple](../e/expand_tuple.md) (for tuple expansion operations)
  - Various functions via MinimalTuple typedef

## Notes and Other Information
- Fields from t_infomask2 onward must exactly match the corresponding fields in HeapTupleHeaderData to ensure compatibility
- The structure is 23 bytes plus the variable-length NULL bitmap, same footer size as HeapTupleHeaderData
- Commonly used for temporary tuples during sorting, aggregation, and other intermediate processing
- The t_len field allows efficient tuple length determination without scanning the entire tuple
- MINIMAL_TUPLE_PADDING ensures proper memory alignment across different platforms
- Size calculation available through SizeofMinimalTupleHeader macro
- Located in src/include/access/htup_details.h:629-648 with typedef in src/include/access/htup.h:25