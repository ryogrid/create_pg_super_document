# NamedTuplestoreScanNext

## Location
[src/backend/executor/nodeNamedtuplestorescan.c:31-51](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeNamedtuplestorescan.c#L31-L51)

## Overview
A static helper function that retrieves the next tuple from a named tuple store scan, serving as the core workhorse for ExecNamedTuplestoreScan.

## Definition

```c
static TupleTableSlot *
NamedTuplestoreScanNext(NamedTuplestoreScanState *node)
```
## Detailed Description
NamedTuplestoreScanNext is a static function that performs the actual tuple retrieval from a named tuple store during a scan operation. It ensures forward-only scanning by asserting the scan direction, selects the appropriate read pointer for the tuple store, and fetches the next tuple into the scan tuple slot. The function returns the tuple slot containing the fetched tuple, or NULL if no more tuples are available.

The function operates on the tuple store referenced by the NamedTuplestoreScanState node, using the node's read pointer to maintain proper position tracking within the store. It intentionally does not support backward scanning, as indicated by the assertion check.

## Parameters / Member Variables
- : Pointer to NamedTuplestoreScanState containing the scan state, tuple store relation, and read pointer for the named tuple store scan operation

## Dependencies
- Functions called/Symbols referenced:
  - ScanDirectionIsForward: Verifies that scanning is in forward direction
  - [tuplestore_select_read_pointer](../t/tuplestore_select_read_pointer.md): Selects the appropriate read pointer for the tuple store
  - [tuplestore_gettupleslot](../t/tuplestore_gettupleslot.md): Retrieves the next tuple from the tuple store into the provided slot
- Called from (representative examples):
  - [ExecNamedTuplestoreScan](../E/ExecNamedTuplestoreScan.md): Main execution function that uses this helper to fetch tuples

## Notes and Other Information
- This is a static function, meaning it's only accessible within the nodeNamedtuplestorescan.c file
- The function explicitly does not support backward scanning, enforced by an assertion
- The function always operates on the scan tuple slot (ss_ScanTupleSlot) from the node's ScanState
- Uses tuple store infrastructure to manage persistent tuple storage and retrieval
- Returns the same slot regardless of whether a tuple was found (caller must check if slot contains data)