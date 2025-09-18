BrinTuple

## Overview
BrinTuple is the on-disk storage format for BRIN index tuples, providing a compact representation with bit-packed metadata and variable-length data sections.

## Definition
typedef struct BrinTuple
{
    /* heap block number that the tuple is for */
    BlockNumber bt_blkno;
    
    /* ---------------
     * bt_info is laid out in the following fashion:
     *
     * 7th (high) bit: has nulls
     * 6th bit: is placeholder tuple
     * 5th bit: range is empty
     * 4-0 bit: offset of data
     * ---------------
     */
    uint8       bt_info;
} BrinTuple;

## Detailed Description
BrinTuple represents the on-disk storage format for BRIN index tuples and is optimized for space efficiency. The structure uses bit packing in the bt_info field to encode multiple boolean flags and a data offset within a single byte. Following the fixed header, the tuple may contain a nulls bitmask (with 2 bits per indexed column) and then opclass-specific Datum values. The compact design minimizes storage overhead while providing all necessary metadata for tuple interpretation. The structure supports placeholder tuples for index maintenance operations and can represent empty ranges where no actual data exists.

## Parameters / Member Variables
- `bt_blkno`: Block number in the heap that this BRIN index tuple summarizes
- `bt_info`: Bit-packed field containing multiple pieces of metadata:
  - Bit 7 (high): Indicates whether the tuple contains any null values
  - Bit 6: Indicates this is a placeholder tuple used during index operations
  - Bit 5: Indicates the range represents no actual tuples (empty range)
  - Bits 4-0: Offset to the start of the variable-length data section

## Dependencies
- Functions called/Symbols referenced:
  - BlockNumber (data type)
  - uint8 (data type)
- Called from (representative examples):
  - [brin_form_tuple](../b/brin_form_tuple.md)
  - [brin_doupdate](../b/brin_doupdate.md)
  - [brin_doinsert](../b/brin_doinsert.md)
  - [brinGetTupleForHeapBlock](../b/brinGetTupleForHeapBlock.md)
  - [brin_evacuate_page](../b/brin_evacuate_page.md)
  - [brin_free_tuple](../b/brin_free_tuple.md)
  - [brin_copy_tuple](../b/brin_copy_tuple.md)
  - [brin_deform_tuple](../b/brin_deform_tuple.md)
  - SizeOfBrinTuple (macro)
  - BrinTupleHasNulls (macro)
  - BrinTupleIsPlaceholder (macro)
  - BrinTupleIsEmptyRange (macro)

## Notes and Other Information
- The on-disk format is followed by a variable-length nulls bitmask (2 bits per column) and opclass-defined Datum values
- Bit manipulation macros (SizeOfBrinTuple, BrinTupleHasNulls, etc.) provide convenient access to the packed bt_info field
- Used extensively in BRIN index storage, retrieval, and maintenance operations
- The compact format is crucial for BRINs space efficiency, especially when summarizing large page ranges
- Placeholder tuples facilitate atomic index updates and maintenance operations
- Empty range tuples handle edge cases where page ranges contain no actual data
- The data offset in bt_info allows for efficient navigation to the variable-length portion of the tuple