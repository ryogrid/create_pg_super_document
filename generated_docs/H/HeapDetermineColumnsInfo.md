# HeapDetermineColumnsInfo

## Location
src/backend/access/heap/heapam.c: 4354 - 4443

## Overview
HeapDetermineColumnsInfo analyzes two heap tuples to determine which columns have been modified, returning a bitmapset of changed columns from those specified as interesting for HOT (Heap-Only Tuple) update optimization.

## Definition


## Detailed Description
This function compares old and new tuple versions to identify modified columns, which is crucial for HOT update decisions. It iterates through columns marked as interesting (typically indexed columns) and performs attribute-by-attribute comparison. The function handles special cases for whole-tuple references and system attributes, and tracks whether any unmodified attributes are stored externally.

The comparison process involves:
1. Iterating through interesting columns using bitmapset operations
2. Handling special cases (whole-tuple refs, system attributes except tableOID)
3. Extracting attribute values from both tuples
4. Comparing values using heap_attr_equals
5. Checking for external storage in unmodified variable-length attributes

## Parameters / Member Variables
- : Relation descriptor for the table being updated
- : Bitmapset of column indices to examine (typically indexed columns)
- : Bitmapset of columns that may be stored externally
- : Original heap tuple before update
- : New heap tuple after update
- : Output parameter set to true if any unmodified interesting attribute is stored externally

## Dependencies
- Functions called/Symbols referenced:
  - [bms_next_member](../b/bms_next_member.md) (iterate through bitmapset)
  - [bms_add_member](../b/bms_add_member.md) (add column to modified set)
  - [heap_getattr](../h/heap_getattr.md) (extract attribute values from tuples)
  - [heap_attr_equals](../h/heap_attr_equals.md) (compare attribute values)
  - [bms_is_member](../b/bms_is_member.md) (check bitmapset membership)
  - VARATT_IS_EXTERNAL (check if variable-length attribute is stored externally)
  - RelationGetDescr (get tuple descriptor)
  - TupleDescAttr (access attribute descriptor)
- Called from (representative examples):
  - [heap_update](../h/heap_update.md)

## Notes and Other Information
- Critical component of HOT update optimization in PostgreSQL
- Performance consideration: inefficient for many indexed columns due to repeated heap_getattr calls
- Automatically marks whole-tuple references and most system attributes as modified
- Only tableOID system attribute is properly compared
- Tracks external storage to help determine HOT eligibility
- Returns NULL bitmapset if no columns were modified
- Part of PostgreSQL's heap access method for update optimization