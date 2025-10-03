# expand_tuple

## Location
[src/backend/access/common/heaptuple.c:828-1052](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/heaptuple.c#L828-L1052)

## Overview
Internal static function that expands a tuple with fewer attributes to a target tuple descriptor by filling in missing values or NULLs for absent attributes.

## Definition

```c
structure are allocated in one chunk.
	 */
	if (targetHeapTuple)
	{
		len += offsetof(HeapTupleHeaderData, t_bits);
		hoff = len = MAXALIGN(len); /* align user data safely */
		len += targetDataLen;

		*targetHeapTuple = (HeapTuple) palloc0(HEAPTUPLESIZE + len);
		(*targetHeapTuple)->t_data
			= targetTHeader
			= (HeapTupleHeader) ((char *) *targetHeapTuple + HEAPTUPLESIZE);
		(*targetHeapTuple)->t_len = len;
		(*targetHeapTuple)->t_tableOid = sourceTuple->t_tableOid;
		(*targetHeapTuple)->t_self = sourceTuple->t_self;

		targetTHeader->t_infomask = sourceTHeader->t_infomask;
		targetTHeader->t_hoff = hoff;
		HeapTupleHeaderSetNatts(targetTHeader, natts);
		HeapTupleHeaderSetDatumLength(targetTHeader, len);
		HeapTupleHeaderSetTypeId(targetTHeader, tupleDesc->tdtypeid);
		HeapTupleHeaderSetTypMod(targetTHeader, tupleDesc->tdtypmod);
		/* We also make sure that t_ctid is invalid unless explicitly set */
		ItemPointerSetInvalid(&(targetTHeader->t_ctid));
		if (targetNullLen > 0)
			nullBits = (bits8 *) ((char *) (*targetHeapTuple)->t_data
								  + offsetof(HeapTupleHeaderData, t_bits));
		targetData = (char *) (*targetHeapTuple)->t_data + hoff;
		infoMask = &(targetTHeader->t_infomask);
	}
	else
	{
		len += SizeofMinimalTupleHeader;
		hoff = len = MAXALIGN(len); /* align user data safely */
		len += targetDataLen;

		*targetMinimalTuple = (MinimalTuple) palloc0(len);
		(*targetMinimalTuple)->t_len = len;
		(*targetMinimalTuple)->t_hoff = hoff + MINIMAL_TUPLE_OFFSET;
		(*targetMinimalTuple)->t_infomask = sourceTHeader->t_infomask;
		/* Same macro works for MinimalTuples */
		HeapTupleHeaderSetNatts(*targetMinimalTuple, natts);
		if (targetNullLen > 0)
			nullBits = (bits8 *) ((char *) *targetMinimalTuple
								  + offsetof(MinimalTupleData, t_bits));
		targetData = (char *) *targetMinimalTuple + hoff;
		infoMask = &((*targetMinimalTuple)->t_infomask);
	}

	if (targetNullLen > 0)
	{
		if (sourceNullLen > 0)
		{
			/* if bitmap pre-existed copy in - all is set */
			memcpy(nullBits,
				   ((char *) sourceTHeader)
				   + offsetof(HeapTupleHeaderData, t_bits),
				   sourceNullLen);
			nullBits += sourceNullLen - 1;
		}
		else
		{
			sourceNullLen = BITMAPLEN(sourceNatts);
			/* Set NOT NULL for all existing attributes */
			memset(nullBits, 0xff, sourceNullLen);

			nullBits += sourceNullLen - 1;

			if (sourceNatts & 0x07)
			{
				/* build the mask (inverted!) */
				bitMask = 0xff << (sourceNatts & 0x07);
				/* Voila */
				*nullBits = ~bitMask;
			}
		}

		bitMask = (1 << ((sourceNatts - 1) & 0x07));
	}							/* End if have null bitmap */

	memcpy(targetData,
		   ((char *) sourceTuple->t_data) + sourceTHeader->t_hoff,
		   sourceDataLen);
```
## Detailed Description
The  function is a core internal function that handles tuple expansion when a source tuple has fewer attributes than required by a target tuple descriptor. This situation commonly occurs during schema evolution when new columns are added to tables and existing tuples need to be logically expanded to match the new schema.

The function can create either a HeapTuple or MinimalTuple as output (exactly one target parameter must be non-NULL). For missing attributes, it uses default values from the tuple descriptor's constraint information if available, otherwise it sets them to NULL. The function handles complex memory layout calculations including null bitmap management, data alignment, and proper tuple header initialization.

The expansion process involves calculating the required memory size, allocating and initializing the target tuple structure, copying existing attribute data, and filling in missing attributes with appropriate values or NULLs.

## Parameters
- `targetHeapTuple`: Pointer to HeapTuple pointer for output (NULL if creating MinimalTuple)
- `targetMinimalTuple`: Pointer to MinimalTuple pointer for output (NULL if creating HeapTuple)
- `sourceTuple`: The source HeapTuple with fewer attributes that needs expansion
- `tupleDesc`: Tuple descriptor defining the target schema with more attributes

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleHasNulls (null bitmap checking)
  - HeapTupleHeaderGetNatts (attribute count extraction)
  - BITMAPLEN (null bitmap size calculation)
  - att_align_datum (data alignment)
  - att_addlength_pointer (length calculation)
  - [palloc0](../p/palloc0.md) (zero-initialized memory allocation)
  - HeapTupleHeaderSetNatts (attribute count setting)
  - HeapTupleHeaderSetDatumLength (length setting)
  - HeapTupleHeaderSetTypeId (type ID setting)
  - HeapTupleHeaderSetTypMod (type modifier setting)
  - [ItemPointerSetInvalid](../I/ItemPointerSetInvalid.md) (tuple ID initialization)
  - [fill_val](../f/fill_val.md) (attribute value filling)
- Called from (representative examples):
  - [minimal_expand_tuple](../m/minimal_expand_tuple.md)
  - [heap_expand_tuple](../h/heap_expand_tuple.md)

## Notes and Other Information
- Static function - not part of the public API
- Requires exactly one of targetHeapTuple or targetMinimalTuple to be non-NULL
- Source tuple must have fewer attributes than the target tuple descriptor
- Handles complex memory layout with proper alignment and null bitmap management
- Uses AttrMissing information from tuple descriptor constraints for default values
- Critical for schema evolution and backward compatibility
- Located in src/backend/access/common/heaptuple.c:828-1052

## Simplified Source

```c
static void expand_tuple(HeapTuple *targetHeapTuple,
                        MinimalTuple *targetMinimalTuple,
                        HeapTuple sourceTuple,
                        TupleDesc tupleDesc)
{
    // Extract basic tuple information
    HeapTupleHeader sourceHeader = sourceTuple->t_data;
    int sourceNatts = HeapTupleHeaderGetNatts(sourceHeader);
    int targetNatts = tupleDesc->natts;
    bool hasNulls = HeapTupleHasNulls(sourceTuple);

    // Calculate space needed for missing attributes
    Size sourceDataLen = sourceTuple->t_len - sourceHeader->t_hoff;
    Size targetDataLen = sourceDataLen;

    // Add space for missing values if they exist
    AttrMissing *missing_attrs = NULL;
    if (tupleDesc->constr && tupleDesc->constr->missing) {
        missing_attrs = tupleDesc->constr->missing;

        // Calculate additional space needed for missing attributes
        for (int attnum = sourceNatts; attnum < targetNatts; attnum++) {
            if (missing_attrs[attnum].am_present) {
                Form_pg_attribute attr = TupleDescAttr(tupleDesc, attnum);
                targetDataLen = att_align_datum(targetDataLen, attr->attalign,
                                               attr->attlen, missing_attrs[attnum].am_value);
                targetDataLen = att_addlength_pointer(targetDataLen, attr->attlen,
                                                     missing_attrs[attnum].am_value);
            } else {
                hasNulls = true;
            }
        }
    } else {
        hasNulls = true; // Missing attributes will be NULL
    }

    // Allocate target tuple (HeapTuple or MinimalTuple)
    Size totalLen = (hasNulls ? BITMAPLEN(targetNatts) : 0);
    if (targetHeapTuple) {
        totalLen += offsetof(HeapTupleHeaderData, t_bits);
        totalLen = MAXALIGN(totalLen) + targetDataLen;
        *targetHeapTuple = (HeapTuple) palloc0(HEAPTUPLESIZE + totalLen);
        // Initialize HeapTuple fields...
    } else {
        totalLen += SizeofMinimalTupleHeader;
        totalLen = MAXALIGN(totalLen) + targetDataLen;
        *targetMinimalTuple = (MinimalTuple) palloc0(totalLen);
        // Initialize MinimalTuple fields...
    }

    // Copy existing data from source tuple
    char *targetData = /* calculated target data location */;
    memcpy(targetData, ((char *) sourceTuple->t_data) + sourceHeader->t_hoff, sourceDataLen);

    // Fill in missing attributes with default values or NULLs
    for (int attnum = sourceNatts; attnum < targetNatts; attnum++) {
        Form_pg_attribute attr = TupleDescAttr(tupleDesc, attnum);
        if (missing_attrs && missing_attrs[attnum].am_present) {
            // Use provided default value
            fill_val(attr, /* null bitmap params */, missing_attrs[attnum].am_value, false);
        } else {
            // Set to NULL
            fill_val(attr, /* null bitmap params */, (Datum) 0, true);
        }
    }
}
```