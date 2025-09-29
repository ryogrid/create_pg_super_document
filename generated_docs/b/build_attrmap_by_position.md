# build_attrmap_by_position

## Location
[src/backend/access/common/attmap.c:75-176](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/attmap.c#L75-L176)

## Overview
Builds an attribute map for tuple conversion by matching input and output columns by their physical positions, validating type compatibility and handling dropped columns appropriately.

## Definition
```c
AttrMap *build_attrmap_by_position(TupleDesc indesc, TupleDesc outdesc, const char *msg)
```

## Detailed Description
The `build_attrmap_by_position` function creates an attribute map that matches columns between two tuple descriptors based on their physical positions, ignoring dropped columns. It performs comprehensive validation including type and typemod compatibility checking, and reports detailed error messages if mismatches are found. The function is designed as a subroutine for `convert_tuples_by_position` but can be used standalone. 

The function follows a careful algorithm: it iterates through each non-dropped column in the output descriptor, finds the corresponding non-dropped column in the input descriptor by position, validates type compatibility, and builds the mapping. If all columns match perfectly (one-to-one), it returns NULL to indicate no conversion is needed, otherwise it returns the attribute map.

## Parameters / Member Variables
- `indesc`: Input tuple descriptor (the "returned" rowtype in error messages)
- `outdesc`: Output tuple descriptor (the "expected" rowtype in error messages) 
- `msg`: Error message context string used in error reports

## Dependencies
- Functions called/Symbols referenced:
  - [make_attrmap](../m/make_attrmap.md) (creates the basic attribute map structure)
  - [format_type_with_typemod](../f/format_type_with_typemod.md) (formats type names for error messages)
  - [check_attrmap_match](../c/check_attrmap_match.md) (checks for one-to-one mapping)
  - [free_attrmap](../f/free_attrmap.md) (frees the map if no conversion needed)
  - `ereport` (error reporting)
  - `TupleDescAttr` (accesses tuple descriptor attributes)
- Called from (representative examples):
  - [convert_tuples_by_position](../c/convert_tuples_by_position.md)

## Notes and Other Information
- Dropped columns are ignored in both input and output and marked as 0 in the mapping
- Performs strict type and typemod validation - mismatches result in detailed error messages
- Returns NULL if no runtime conversion is needed (perfect one-to-one match)
- The algorithm counts non-dropped attributes separately for accurate error reporting
- Error messages refer to indesc as "returned" and outdesc as "expected" rowtype
- Used primarily in tuple conversion scenarios where column order is preserved
- Located in `src/backend/access/common/attmap.c:75-176`

## Simplified Source

```c
AttrMap *build_attrmap_by_position(TupleDesc indesc, TupleDesc outdesc, const char *msg)
{
    AttrMap *attrMap;
    int nincols, noutcols;
    int n, i, j;
    bool same = true;

    // Create attribute map based on output descriptor length
    n = outdesc->natts;
    attrMap = make_attrmap(n);

    j = 0;              // Next physical input attribute
    nincols = noutcols = 0;  // Count non-dropped attributes

    // Map output columns to input columns by position
    for (i = 0; i < n; i++)
    {
        Form_pg_attribute out_att = TupleDescAttr(outdesc, i);

        if (out_att->attisdropped)
            continue;  // attrMap->attnums[i] already 0

        noutcols++;

        // Find next non-dropped input column
        for (; j < indesc->natts; j++)
        {
            Form_pg_attribute in_att = TupleDescAttr(indesc, j);
            if (in_att->attisdropped)
                continue;

            nincols++;

            // Check type compatibility
            if (out_att->atttypid != in_att->atttypid ||
                (out_att->atttypmod != in_att->atttypmod && out_att->atttypmod >= 0))
            {
                ereport(ERROR,
                        (errcode(ERRCODE_DATATYPE_MISMATCH),
                         errmsg_internal("%s", _(msg)),
                         errdetail("Returned type %s does not match expected type %s in column %d.",
                                  format_type_with_typemod(in_att->atttypid, in_att->atttypmod),
                                  format_type_with_typemod(out_att->atttypid, out_att->atttypmod),
                                  noutcols)));
            }

            // Map this output column to input column (1-based)
            attrMap->attnums[i] = (AttrNumber)(j + 1);
            j++;
            break;
        }

        if (attrMap->attnums[i] == 0)
            same = false;  // Missing input column
    }

    // Check for excess input columns
    for (; j < indesc->natts; j++)
    {
        if (!TupleDescAttr(indesc, j)->attisdropped)
        {
            nincols++;
            same = false;
        }
    }

    // Report column count mismatch
    if (!same)
        ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                 errmsg_internal("%s", _(msg)),
                 errdetail("Number of returned columns (%d) does not match expected column count (%d).",
                          nincols, noutcols)));

    // Check if conversion is actually needed
    if (check_attrmap_match(indesc, outdesc, attrMap))
    {
        // Perfect match - no runtime conversion needed
        free_attrmap(attrMap);
        return NULL;
    }

    return attrMap;
}
```