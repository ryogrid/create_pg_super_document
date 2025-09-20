# toast_tuple_find_biggest_attribute

## Location
[src/backend/access/table/toast_helper.c:181-226](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/table/toast_helper.c#L181-L226)

## Overview
Finds the largest varlena attribute in a tuple that meets specific criteria for compression or externalization operations during the TOAST process.

## Definition

```c
int
toast_tuple_find_biggest_attribute(ToastTupleContext *ttc,
								   bool for_compression, bool check_main)
```
## Detailed Description
This function searches through all attributes in a tuple to find the largest one that satisfies specific criteria for TOAST operations. It serves as a helper function to determine which attribute should be processed next during tuple compression or externalization.

The function applies several filters to determine eligible attributes:
- Skips attributes marked with TOASTCOL_IGNORE
- If searching for compression candidates, skips TOASTCOL_INCOMPRESSIBLE attributes
- Skips already external or compressed values
- Filters by storage type (MAIN vs EXTENDED/EXTERNAL) based on the check_main parameter
- Only considers attributes larger than MAXALIGN(TOAST_POINTER_SIZE)

The function returns the array index of the largest qualifying attribute, enabling the caller to target the most beneficial attribute for the next TOAST operation.

## Parameters / Member Variables
- : ToastTupleContext containing tuple data and attribute metadata
- : If true, searches for compression candidates and excludes incompressible attributes
- : If true, only considers TYPSTORAGE_MAIN attributes; if false, considers EXTENDED/EXTERNAL attributes

## Dependencies
- Functions called/Symbols referenced:
  - TupleDescAttr
  - [DatumGetPointer](../D/DatumGetPointer.md)
  - VARATT_IS_EXTERNAL
  - VARATT_IS_COMPRESSED
  - MAXALIGN
  - TOAST_POINTER_SIZE
  - TOASTCOL_IGNORE
  - TOASTCOL_INCOMPRESSIBLE
  - TYPSTORAGE_MAIN
  - TYPSTORAGE_EXTENDED  
  - TYPSTORAGE_EXTERNAL
- Called from (representative examples):
  - [heap_toast_insert_or_update](../h/heap_toast_insert_or_update.md) (multiple calls for different phases)

## Notes and Other Information
- Returns -1 if no suitable attribute is found
- Critical for implementing PostgreSQL's TOAST strategy of processing largest attributes first
- The minimum size threshold (TOAST_POINTER_SIZE) ensures that compression/externalization will actually save space
- Different storage types correspond to different TOAST strategies: MAIN (prefer compression), EXTENDED (prefer externalization), EXTERNAL (externalization only)
- Used multiple times during the TOAST process to iteratively find the next best candidate for processing