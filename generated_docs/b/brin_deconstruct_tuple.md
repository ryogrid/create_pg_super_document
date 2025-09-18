# brin_deconstruct_tuple

## Location
src/backend/access/brin/brin_tuple.c: 645 - 720

## Overview
Extracts attribute values and null flags from the raw data area of an on-disk BRIN tuple, handling the complex parsing of compressed tuple format.

## Definition
static inline void brin_deconstruct_tuple(BrinDesc *brdesc, char *tp, bits8 *nullbits, bool nulls, Datum *values, bool *allnulls, bool *hasnulls)

## Detailed Description
This function performs the core attribute extraction from a BRIN tuple's on-disk format. It operates in two phases: first extracting null flag information for each attribute, then iterating through the actual data values. The function handles the specialized BRIN tuple format where null information is encoded as both "all nulls" (entire page range is null) and "has nulls" (some values are null) flags. It uses the disk tuple descriptor to properly align and extract variable-length and fixed-length attributes, accounting for the packed storage format used by BRIN tuples.

## Parameters / Member Variables
- brdesc: Pointer to BrinDesc structure containing tuple descriptor and storage metadata
- tp: Pointer to the tuple data area containing the packed attribute values  
- nullbits: Pointer to the null bitmask within the tuple
- nulls: Boolean indicating whether the tuple contains any null information
- values: Output array to store extracted Datum values (size brdesc->bd_totalstored)
- allnulls: Output array for "all nulls" flags (size brdesc->bd_tupdesc->natts)
- hasnulls: Output array for "has nulls" flags (size brdesc->bd_tupdesc->natts)

## Dependencies
- Functions called/Symbols referenced:
  - att_isnull (checks if attribute is null in bitmask)
  - brtuple_disk_tupdesc (gets disk format tuple descriptor)
  - att_align_pointer (aligns pointer for variable-length attributes)
  - att_align_nominal (aligns offset for fixed-length attributes)
  - fetchatt (extracts attribute value from tuple data)
  - att_addlength_pointer (advances pointer past attribute data)
  - TupleDescAttr (accesses tuple descriptor attributes)
- Called from (representative examples):
  - brin_deform_tuple
  - TOAST_INDEX_HACK

## Notes and Other Information
- Uses reversed sense of att_isnull test compared to normal tuples (1 means null rather than not-null)
- Handles both "allnulls" and "hasnulls" flags stored in the same null bitmask at different offsets
- Must handle variable-length attributes with proper alignment and length calculation
- Cannot cache offsets since attribute entries may be reused for multiple columns
- Output arrays must be pre-allocated by the caller with proper sizes
- Static inline function optimized for performance in tuple processing