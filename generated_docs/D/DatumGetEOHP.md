# DatumGetEOHP

## Location
[src/backend/utils/adt/expandeddatum.c:29-47](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/expandeddatum.c#L29-L47)

## Overview
Extracts an ExpandedObjectHeader pointer from a Datum that contains an expanded-object reference, handling potential alignment issues in the process.

## Definition

```c
ExpandedObjectHeader *
DatumGetEOHP(Datum d)
```
## Detailed Description
DatumGetEOHP is a utility function that safely extracts an ExpandedObjectHeader pointer from a Datum containing an expanded-object reference. The function handles the complexity of extracting the pointer when it may not be properly aligned, similar to how VARATT_EXTERNAL_GET_POINTER() works.

The function first casts the Datum to a varattrib_1b_e pointer, then uses memcpy to safely copy the expanded object pointer from the variable-length attribute data, avoiding potential alignment issues. It includes assertions to verify that the input is indeed an external expanded reference and that the extracted pointer is valid.

## Parameters / Member Variables
- `d`: The Datum containing an expanded-object reference to be extracted
## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetPointer](DatumGetPointer.md) (macro)
  - VARATT_IS_EXTERNAL_EXPANDED (macro)
  - VARDATA_EXTERNAL (macro)  
  - VARATT_IS_EXPANDED_HEADER (macro)
  - memcpy (standard library function)
- Types referenced:
  - varattrib_1b_e
  - [varatt_expanded](../v/varatt_expanded.md)
  - [ExpandedObjectHeader](../E/ExpandedObjectHeader.md)
- Called from (representative examples):
  - [detoast_external_attr](../d/detoast_external_attr.md)
  - [MakeExpandedObjectReadOnlyInternal](../M/MakeExpandedObjectReadOnlyInternal.md)
  - [DatumGetExpandedArray](DatumGetExpandedArray.md)
  - [DatumGetExpandedRecord](DatumGetExpandedRecord.md)
  - [datumCopy](../d/datumCopy.md)

## Notes and Other Information
- This function includes safety assertions to verify the input Datum is actually an expanded-object reference
- The use of memcpy is deliberate to handle potential pointer alignment issues
- This is a fundamental utility function used throughout PostgreSQL's expanded object system
- The function is defined in src/backend/utils/adt/expandeddatum.c:29-47

## Simplified Source

```c
ExpandedObjectHeader *DatumGetEOHP(Datum d) {
    varattrib_1b_e *datum = (varattrib_1b_e *) DatumGetPointer(d);
    varatt_expanded ptr;

    // Verify input is an expanded object reference
    Assert(VARATT_IS_EXTERNAL_EXPANDED(datum));

    // Safely copy pointer to handle alignment issues
    memcpy(&ptr, VARDATA_EXTERNAL(datum), sizeof(ptr));

    // Verify extracted pointer is valid expanded header
    Assert(VARATT_IS_EXPANDED_HEADER(ptr.eohptr));

    return ptr.eohptr;
}
```