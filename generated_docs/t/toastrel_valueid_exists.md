# toastrel_valueid_exists

## Location
[src/backend/access/common/toast_internals.c:461-508](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/toast_internals.c#L461-L508)

## Overview
Tests whether a toast value with a given ID exists in the specified toast relation, considering both live and dead tuples for safety.

## Definition

```c
static bool
toastrel_valueid_exists(Relation toastrel, Oid valueid)
```
## Detailed Description
This internal function performs a lookup to determine if any toast chunks exist for a specified value ID within a given toast relation. It uses the toast relation's primary index to efficiently search for tuples matching the target value ID. The function is designed with safety in mind, using SnapshotAny to detect both live and dead tuples, which is important for avoiding OID reuse conflicts during operations like table rewrites.

The function follows the same safety principles as GetNewOidWithIndex() by considering dead tuples as existing values, preventing potential issues that could arise from premature OID reuse. This conservative approach ensures data integrity during complex operations where multiple versions of data might temporarily coexist.

## Parameters / Member Variables
- : The toast relation to search within
- : The OID of the toast value to search for

## Dependencies
- Functions called/Symbols referenced:
  - [toast_open_indexes](toast_open_indexes.md)
  - [toast_close_indexes](toast_close_indexes.md)
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [systable_endscan](../s/systable_endscan.md)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - RelationGetRelid
  - SnapshotAny
- Called from (representative examples):
  - [toast_save_datum](toast_save_datum.md)
  - [toastid_valueid_exists](toastid_valueid_exists.md)

## Notes and Other Information
- Declared as static function, internal to toast_internals.c
- Uses SnapshotAny to see both live and dead tuples for safety considerations
- Maintains RowExclusiveLock on toast indexes during the scan operation
- Returns true if any chunk with the specified value ID is found, false otherwise
- Designed for use during OID generation to prevent conflicts during table rewrite scenarios
- Only needs to find one matching tuple to confirm existence, making it efficient
- Follows PostgreSQL's general principle of conservative OID management to avoid reuse issues