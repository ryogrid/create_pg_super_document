# IndexSupportsBackwardScan

## Location
[src/backend/executor/execAmi.c:602-634](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execAmi.c#L602-L634)

## Overview
IndexSupportsBackwardScan is a static function that determines whether a specific index supports backward scanning by checking the index access method's capabilities.

## Definition

```c
struct */
	amroutine = GetIndexAmRoutineByAmId(idxrelrec->relam, false);
```
## Detailed Description
This function queries the system catalog to determine if an index supports backward scanning. It looks up the index relation in pg_class, retrieves the access method information, and checks the  flag in the IndexAmRoutine structure. The function is used internally by the executor to determine scan capabilities for IndexScan and IndexOnlyScan operations.

The function performs the following steps:
1. Looks up the index relation in the pg_class system catalog using the provided OID
2. Extracts the access method ID from the relation record
3. Retrieves the IndexAmRoutine structure for the access method
4. Checks the  flag to determine backward scan support
5. Cleans up allocated memory and releases system cache references

## Parameters / Member Variables
- : The OID of the index relation to check for backward scan support

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - elog
  - GETSTRUCT
  - [GetIndexAmRoutineByAmId](../G/GetIndexAmRoutineByAmId.md)
  - [pfree](../p/pfree.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
- Called from (representative examples):
  - [ExecSupportsBackwardScan](../E/ExecSupportsBackwardScan.md) (twice in the same file)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the execAmi.c file
- The function is specifically used for IndexScan and IndexOnlyScan node types
- Error handling is included for cases where the index relation lookup fails
- Memory management is properly handled with pfree() and ReleaseSysCache() calls
- The function relies on the access method's  capability flag

## Simplified Source

```c
static bool IndexSupportsBackwardScan(Oid indexid) {
    bool result;
    HeapTuple ht_idxrel;
    Form_pg_class idxrelrec;
    IndexAmRoutine *amroutine;

    // Look up the index relation in pg_class catalog
    ht_idxrel = SearchSysCache1(RELOID, ObjectIdGetDatum(indexid));
    if (!HeapTupleIsValid(ht_idxrel)) {
        elog(ERROR, "cache lookup failed for relation %u", indexid);
    }

    // Extract the relation record
    idxrelrec = (Form_pg_class) GETSTRUCT(ht_idxrel);

    // Get the access method's API structure
    amroutine = GetIndexAmRoutineByAmId(idxrelrec->relam, false);

    // Check if the access method supports backward scanning
    result = amroutine->amcanbackward;

    // Clean up allocated resources
    pfree(amroutine);
    ReleaseSysCache(ht_idxrel);

    return result;
}
```