# SerialControlData

## Location
src/backend/storage/lmgr/predicate.c: 345 - 350

## Overview
SerialControlData is a control structure that manages the serializable isolation implementation's SLRU (Simple LRU) buffer for tracking serializable transactions.

## Definition


## Detailed Description
SerialControlData serves as the control block for PostgreSQL's serializable snapshot isolation implementation. It maintains critical metadata about the SLRU (Simple LRU) buffer system used to track information about serializable transactions. This structure helps PostgreSQL determine which transaction IDs are still relevant for conflict detection and which pages in the SLRU contain valid data.

The structure is part of the predicate locking mechanism that implements Serializable Snapshot Isolation (SSI), which prevents serialization anomalies by tracking read-write conflicts between concurrent transactions.

## Parameters / Member Variables
- : The newest initialized page in the SLRU buffer, representing the most recently allocated page for storing serializable transaction data
- : The newest (highest) valid transaction ID stored in the SLRU, used to determine the upper bound of tracked transactions
- : The oldest transaction ID that might still be of interest for conflict detection, representing the lower bound of the tracking window

## Dependencies
- Functions called/Symbols referenced: None (this is a data structure)
- Called from (representative examples):
  - SerialControl (as a typedef base)
  - SerialInit
  - PredicateLockShmemSize

## Notes and Other Information
- This structure is used in conjunction with the SLRU mechanism to efficiently manage memory for serializable transaction tracking
- The headPage and transaction ID boundaries help PostgreSQL garbage collect old serialization information that's no longer needed
- Part of the larger predicate locking infrastructure that prevents serialization anomalies in SERIALIZABLE isolation level
- Located in src/backend/storage/lmgr/predicate.c:345-350