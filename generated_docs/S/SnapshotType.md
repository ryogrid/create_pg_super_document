# SnapshotType

## Location
src/include/utils/snapshot.h: 119 - 120

## Overview
SnapshotType is an enumeration that defines the different types of snapshots used in PostgreSQL's transaction visibility and Multi-Version Concurrency Control (MVCC) system.

## Definition


## Detailed Description
SnapshotType encodes the specific semantics of a snapshot, determining how tuple visibility is evaluated. Each type represents a different approach to determining whether a tuple should be visible to a particular operation. The enumeration allows the same snapshot structure to be used across different table access methods without requiring separate callbacks for each access method.

## Parameters / Member Variables
- : Standard MVCC snapshot showing tuples visible according to MVCC rules based on transaction commit status at snapshot time
- : Shows tuples valid "for itself" including effects of committed transactions, previous commands, and current command changes
- : Makes any tuple visible regardless of transaction state
- : Used specifically for TOAST row visibility checks
- : Shows tuples including effects of in-progress transactions, used for tuple visibility with concurrent transaction detection
- : MVCC snapshot variant supporting timetravel context for logical decoding of catalog contents
- : Determines if tuples might be visible to some transaction to identify vacuumable tuples

## Dependencies
- Functions called/Symbols referenced:
  - Used by SnapshotData struct
- Called from (representative examples):
  - Various heap access methods
  - Logical replication components
  - Snapshot management functions

## Notes and Other Information
This enumeration replaced callback-based approaches to allow the same snapshot to work with different table access methods. Each snapshot type has specific semantics documented alongside its enum value, described in terms not specific to individual table access methods. The design enables flexible snapshot behavior while maintaining type safety and performance.