# gistCompressValues

## Location
[src/backend/access/gist/gistutil.c:595-644](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistutil.c#L595-L644)

## Overview
Compresses attribute values for GiST index entries by applying the compress function from the operator class for each key attribute, and includes included attributes for leaf entries.

## Definition

```c
void
gistCompressValues(GISTSTATE *giststate, Relation r,
				   const Datum *attdata, const bool *isnull, bool isleaf, Datum *compatt)
```
## Detailed Description
This function processes attribute data for GiST index entries by applying compression functions defined by the operator classes. For each key attribute, it creates a GISTENTRY, applies the compress function if one is defined in the operator class, and stores the resulting compressed value. For leaf entries, it also handles included attributes by copying them directly without compression. The function is essential for preparing data before storing it in GiST index pages.

## Parameters / Member Variables
- `*giststate`: GiST state information containing operator class functions and collation information
- `r`: The GiST index relation
- `*attdata`: Array of input attribute values (Datums) to be compressed
- `*isnull`: Array of boolean flags indicating which attributes are NULL
- `isleaf`: Boolean flag indicating whether this is for a leaf page entry
- `*compatt`: Output array where compressed attribute values are stored
## Dependencies
- Functions called/Symbols referenced:
  - IndexRelationGetNumberOfKeyAttributes
  - gistentryinit
  - [FunctionCall1Coll](../F/FunctionCall1Coll.md)
  - [GISTENTRY](../G/GISTENTRY.md) (struct)
  - [GISTSTATE](../G/GISTSTATE.md) (struct)
- Called from (representative examples):
  - [gistSortedBuildCallback](gistSortedBuildCallback.md)
  - [gistFormTuple](gistFormTuple.md)

## Notes and Other Information
- The function handles both key attributes (which get compressed) and included attributes (copied directly for leaf entries)
- Compression is optional - if no compress function is defined in the operator class, the original value is used
- NULL values are handled by storing (Datum) 0 in the output array
- For leaf entries, included attributes are processed after key attributes without compression
- The function uses the collation information from giststate when calling compression functions

## Simplified Source

```c
void gistCompressValues(GISTSTATE *giststate, Relation r,
                        const Datum *attdata, const bool *isnull,
                        bool isleaf, Datum *compatt) {
    int num_key_attrs = IndexRelationGetNumberOfKeyAttributes(r);

    // Process key attributes with compression
    for (int i = 0; i < num_key_attrs; i++) {
        if (isnull[i]) {
            compatt[i] = (Datum) 0;
        } else {
            GISTENTRY entry;
            gistentryinit(entry, attdata[i], r, NULL, 0, isleaf);

            // Apply compression if function exists
            if (OidIsValid(giststate->compressFn[i].fn_oid)) {
                GISTENTRY *compressed = (GISTENTRY *)
                    DatumGetPointer(FunctionCall1Coll(&giststate->compressFn[i],
                                                    giststate->supportCollation[i],
                                                    PointerGetDatum(&entry)));
                compatt[i] = compressed->key;
            } else {
                compatt[i] = entry.key;
            }
        }
    }

    // For leaf entries, copy included attributes directly
    if (isleaf) {
        for (int i = num_key_attrs; i < r->rd_att->natts; i++) {
            compatt[i] = isnull[i] ? (Datum) 0 : attdata[i];
        }
    }
}
```