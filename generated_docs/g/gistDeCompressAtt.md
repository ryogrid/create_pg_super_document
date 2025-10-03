# gistDeCompressAtt

## Location
[src/backend/access/gist/gistutil.c:295-314](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistutil.c#L295-L314)

## Overview
The `gistDeCompressAtt` function decompresses all key attributes in a GiST index tuple, preparing them for further processing during index operations.

## Definition
```c
void gistDeCompressAtt(GISTSTATE *giststate, Relation r, IndexTuple tuple, Page p,
                       OffsetNumber o, GISTENTRY *attdata, bool *isnull)
```

## Detailed Description
This function iterates through all key attributes of a GiST index tuple and decompresses each one using the appropriate decompression method defined in the GISTSTATE. It extracts each attribute value from the tuple and initializes a GISTENTRY structure for each attribute through `gistdentryinit`. The function is essential for converting stored tuple data into a format suitable for GiST index operations like searching, insertion, and splitting.

The decompression process ensures that compressed or encoded attribute values are converted back to their working representation, allowing GiST operators to perform their intended operations on the data.

## Parameters / Member Variables
- `giststate`: Pointer to GISTSTATE containing operator information and tuple descriptors for the index
- `r`: The GiST index relation being processed
- `tuple`: The IndexTuple containing the compressed attribute data to be decompressed
- `p`: The page containing the tuple (used for context in decompression)
- `o`: The offset number of the tuple on the page
- `attdata`: Output array of GISTENTRY structures to store the decompressed attribute data
- `isnull`: Output array of boolean flags indicating which attributes are NULL

## Dependencies
- Functions called/Symbols referenced:
  - IndexRelationGetNumberOfKeyAttributes
  - [index_getattr](../i/index_getattr.md)
  - [gistdentryinit](gistdentryinit.md)
- Called from (representative examples):
  - [gistRelocateBuildBuffersOnSplit](gistRelocateBuildBuffersOnSplit.md)
  - [placeOne](../p/placeOne.md)
  - [gistgetadjusted](gistgetadjusted.md)
  - [gistchoose](gistchoose.md)

## Notes and Other Information
- This function processes all key attributes of the tuple in sequence (excluding any included columns)
- The `isnull` array must be pre-allocated with sufficient space for all key attributes
- The `attdata` array must also be pre-allocated with sufficient GISTENTRY slots
- The function uses the leaf tuple descriptor from giststate for attribute extraction
- This is a utility function commonly used in GiST operations that need to work with tuple contents

## Simplified Source

```c
void gistDeCompressAtt(GISTSTATE *giststate, Relation r, IndexTuple tuple, Page p,
                      OffsetNumber o, GISTENTRY *attdata, bool *isnull) {
    // Process each key attribute in the tuple
    for (int i = 0; i < IndexRelationGetNumberOfKeyAttributes(r); i++) {
        Datum datum;

        // Extract attribute value from tuple
        datum = index_getattr(tuple, i + 1, giststate->leafTupdesc, &isnull[i]);

        // Initialize GIST entry with decompressed data
        gistdentryinit(giststate, i, &attdata[i],
                      datum, r, p, o,
                      false, isnull[i]);
    }
}
```