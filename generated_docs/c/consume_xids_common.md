# consume_xids_common

## Location
src/test/modules/xid_wraparound/xid_wraparound.c: 71 - 77

## Overview
A static helper function that provides common XID consumption functionality for testing XID wraparound scenarios in PostgreSQL's test modules.

## Definition


## Detailed Description
The  function serves as the core implementation for consuming (allocating) transaction IDs in PostgreSQL's XID wraparound testing module. It supports two consumption modes: consuming a specific number of XIDs or consuming XIDs until reaching a target XID value. The function employs both a fast shortcut method (direct counter manipulation) and a slow path (individual XID allocation via GetNewTransactionId) to efficiently consume large numbers of transaction IDs for testing purposes.

The function maintains progress reporting by logging a NOTICE message every 10 million consumed XIDs, helping administrators monitor the progress of XID consumption operations. It uses subtransactions to consume XIDs, marking them as subtransactions of the current top-level transaction.

## Parameters / Member Variables
- : Target FullTransactionId to consume up to (used when nxids is 0; InvalidFullTransactionId when consuming a specific count)
- : Number of XIDs to consume (used when untilxid is InvalidFullTransactionId; 0 when consuming until a target XID)

## Dependencies
- Functions called/Symbols referenced:
  - ReadNextFullTransactionId
  - [GetTopTransactionId](../G/GetTopTransactionId.md)  
  - GetNewTransactionId
  - [consume_xids_shortcut](consume_xids_shortcut.md)
  - FullTransactionIdFollowsOrEquals
  - U64FromFullTransactionId
  - EpochFromFullTransactionId
  - XidFromFullTransactionId
  - CHECK_FOR_INTERRUPTS
- Called from (representative examples):
  - [consume_xids](consume_xids.md)
  - [consume_xids_until](consume_xids_until.md)

## Notes and Other Information
- This function is part of the xid_wraparound test module located in src/test/modules/xid_wraparound/
- The function requires an active top-level transaction to work properly since it uses subtransactions
- Progress is reported every REPORT_INTERVAL (10 million) consumed XIDs via elog(NOTICE)
- The fast path (consume_xids_shortcut) is used when more than 2000 XIDs remain and certain conditions are met
- Returns the last allocated FullTransactionId after consumption is complete
- Used exclusively for testing XID wraparound scenarios and should not be used in production code