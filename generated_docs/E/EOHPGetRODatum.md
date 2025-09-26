# EOHPGetRODatum

## Location
[src/include/utils/expandeddatum.h:145-150](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/expandeddatum.h#L145-L150)

## Overview
EOHPGetRODatum is an inline function that extracts a read-only TOAST pointer from an ExpandedObjectHeader and returns it as a Datum.

## Definition
```c
static inline Datum
EOHPGetRODatum(const struct ExpandedObjectHeader *eohptr)
```

## Detailed Description
This function provides access to the read-only TOAST pointer stored within an expanded object's header. Similar to its counterpart EOHPGetRWDatum, this function takes a pointer to an ExpandedObjectHeader and returns the eoh_ro_ptr field as a Datum using PointerGetDatum. The key difference is that this function returns a read-only pointer, which is suitable for functions that only need to read the expanded object's contents without modifying them.

The returned Datum represents a TOAST pointer that provides read-only access to the expanded object. This is part of PostgreSQL's expanded object infrastructure that maintains both read-write and read-only TOAST pointers within the same header, allowing functions to return the appropriate type of pointer without additional allocations and without worrying about object lifespan management.

## Parameters / Member Variables
- `eohptr`: Pointer to an ExpandedObjectHeader structure containing the expanded object metadata and TOAST pointers

## Dependencies
- Functions called/Symbols referenced:
  - [PointerGetDatum](../P/PointerGetDatum.md)
  - [ExpandedObjectHeader](ExpandedObjectHeader.md)
- Called from (representative examples):
  - [MakeExpandedObjectReadOnlyInternal](../M/MakeExpandedObjectReadOnlyInternal.md)
  - [ExpandedRecordGetRODatum](ExpandedRecordGetRODatum.md)

## Notes and Other Information
- This is an inline function defined in src/include/utils/expandeddatum.h:144-148
- The function accesses the eoh_ro_ptr field which is a standard R/O TOAST pointer stored within the ExpandedObjectHeader
- The counterpart function EOHPGetRWDatum provides access to the read-write TOAST pointer
- This function is used when read-only access to the expanded object is sufficient, which can be more efficient and safer than providing write access
- The returned Datum maintains the same lifespan as the underlying ExpandedObjectHeader's memory context
- Having both read-only and read-write pointers available allows PostgreSQL to optimize function calls by providing the minimal necessary access level