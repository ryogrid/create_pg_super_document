# inv_getsize

## Location
src/backend/storage/large_object/inv_api.c: 378 - 425

## Overview
Determines the size of a large object by finding the offset of the last byte plus one, accounting for potential gaps in the data.

## Definition


## Detailed Description
The  function calculates the effective size of a large object by scanning through its pages to find the last byte position. Unlike simple file systems, PostgreSQL large objects can contain gaps (similar to Unix sparse files), so the function returns the offset of the last byte plus one rather than just counting stored bytes.

The function uses a backwards scan of the pg_largeobject index to efficiently locate the last page of the large object. Since the index is ordered by both loid (large object ID) and pageno (page number), a backwards scan with only the loid constraint will visit pages in reverse order, allowing the function to examine just the first (last) valid page to determine the total size.

## Parameters / Member Variables
- : Pointer to the LargeObjectDesc structure containing the large object ID and snapshot information

## Dependencies
- Functions called/Symbols referenced:
  - PointerIsValid (for assertion checking)
  - [open_lo_relation](../o/open_lo_relation.md) (opens the large object catalog relation)
  - [ScanKeyInit](../S/ScanKeyInit.md) (initializes scan key for index search)
  - [systable_beginscan_ordered](../s/systable_beginscan_ordered.md) (begins ordered system table scan)
  - [systable_getnext_ordered](../s/systable_getnext_ordered.md) (gets next tuple in ordered scan)
  - [systable_endscan_ordered](../s/systable_endscan_ordered.md) (ends the system table scan)
  - [getdatafield](../g/getdatafield.md) (extracts data field from large object tuple)
  - HeapTupleHasNulls (checks for null fields)
  - HeapTupleIsValid (validates heap tuple)
- Called from (representative examples):
  - [inv_seek](inv_seek.md)

## Notes and Other Information
- This is a static function, only accessible within the inv_api.c file
- Large objects can have gaps, so size calculation requires finding the actual last byte rather than summing data lengths
- Uses backwards scan optimization to avoid examining all pages of the large object
- The function handles the pg_largeobject relation which stores large object data in chunks
- Returns the size as a uint64 to support large objects exceeding 4GB
- Includes paranoia check for null fields in the pg_largeobject tuple
- The size calculation is: (last_page_number * LOBLKSIZE) + length_of_data_in_last_page