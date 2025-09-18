# table_scan_getnextslot

## Location
[src/include/access/tableam.h:1056-1084](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/tableam.h#L1056-L1084)

## Overview
Retrieves the next tuple from a table scan and stores it in the specified tuple slot, supporting both forward and backward scan directions.

## Definition


## Detailed Description
The  function is the primary interface for retrieving tuples during a table scan operation. It fetches the next tuple from the scan in the specified direction and stores it in the provided tuple slot. The function includes important safety checks, including validation that it's not being called inappropriately during logical decoding operations.

The function sets the table OID in the slot to match the relation being scanned and validates the scan direction. It includes a critical assertion that prevents unexpected calls during logical decoding when CheckXidAlive is valid, as this could lead to inconsistent behavior in catalog or regular table scans.

## Parameters / Member Variables
- : The TableScanDesc structure representing the active scan
- : The scan direction (ForwardScanDirection or BackwardScanDirection)
- : The TupleTableSlot where the retrieved tuple will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [TableScanDesc](../T/TableScanDesc.md) (scan descriptor type)
  - ScanDirection (direction enumeration)
  - TupleTableSlot (tuple storage slot)
  - ForwardScanDirection (forward scan constant)
  - BackwardScanDirection (backward scan constant)
  - RelationGetRelid (get relation OID)
  - TransactionIdIsValid (transaction ID validation)
  - sscan->rs_rd->rd_tableam->scan_getnextslot (table access method get next tuple function)
- Called from (representative examples):
  - [SeqNext](../S/SeqNext.md) (src/backend/executor/nodeSeqscan.c:80)
  - [systable_getnext](../s/systable_getnext.md) (src/backend/access/index/genam.c:532)
  - [DoCopyTo](../D/DoCopyTo.md) (src/backend/commands/copyto.c:859)
  - [ATRewriteTable](../A/ATRewriteTable.md) (src/backend/commands/tablecmds.c:6185)

## Notes and Other Information
- This is an inline function defined in the table access method header file
- Returns true if a tuple was successfully retrieved, false if scan is complete
- Sets the table OID in the slot for proper tuple identification
- Includes safety assertions to prevent misuse during logical decoding
- NoMovementScanDirection is not supported for actual scanning operations
- Critical function in the scan execution path, called repeatedly to iterate through table contents
- Part of PostgreSQL's table access method (TAM) abstraction layer
- The function delegates to the specific table access method's implementation
- Used extensively throughout PostgreSQL for tuple retrieval in various scan contexts