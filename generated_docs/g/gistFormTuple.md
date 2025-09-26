# gistFormTuple

## Location
[src/backend/access/gist/gistutil.c:574-594](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistutil.c#L574-L594)

## Overview
The `gistFormTuple` function creates a GiST IndexTuple from arrays of attribute values and null indicators, applying compression and setting appropriate tuple characteristics.

## Definition
```c
IndexTuple gistFormTuple(GISTSTATE *giststate, Relation r,
                         const Datum *attdata, const bool *isnull, bool isleaf)
```

## Detailed Description
This function constructs a new GiST IndexTuple by first compressing the provided attribute values using the appropriate compression functions from the operator classes, then creating the tuple using the standard index_form_tuple function. It handles both leaf and non-leaf tuples by selecting the appropriate tuple descriptor based on the `isleaf` parameter.

The function performs a critical step in GiST tuple creation by ensuring that attribute values are properly compressed according to the operator class specifications before being stored. For non-leaf (internal) tuples, it also sets a special offset number of 0xffff for historical reasons, as the offset number is unused on internal pages.

The compression step is essential for space efficiency and ensures that the stored representation matches what GiST operators expect when processing the tuple later.

## Parameters / Member Variables
- `giststate`: Pointer to GISTSTATE containing operator class information and tuple descriptors
- `r`: The GiST index relation for which the tuple is being created
- `attdata`: Array of Datum values for each key attribute of the tuple
- `isnull`: Array of boolean flags indicating which attributes are NULL
- `isleaf`: Boolean indicating whether this is a leaf-level tuple (affects tuple descriptor selection)

## Dependencies
- Functions called/Symbols referenced:
  - [gistCompressValues](gistCompressValues.md)
  - [index_form_tuple](../i/index_form_tuple.md)
  - [ItemPointerSetOffsetNumber](../I/ItemPointerSetOffsetNumber.md)
- Called from (representative examples):
  - [gistinsert](gistinsert.md)
  - [gistSplit](gistSplit.md)
  - [gistBuildCallback](gistBuildCallback.md)
  - [gistunion](gistunion.md)
  - [gistgetadjusted](gistgetadjusted.md)

## Notes and Other Information
- Uses INDEX_MAX_KEYS for the compressed attribute array size
- Selects between leafTupdesc and nonLeafTupdesc based on the isleaf parameter
- Sets offset number to 0xffff for internal (non-leaf) pages for historical reasons
- The compression step is mandatory and uses operator class-specific compression functions
- Essential for creating properly formatted GiST tuples ready for storage
- The resulting tuple can be inserted into GiST pages or used in further index operations