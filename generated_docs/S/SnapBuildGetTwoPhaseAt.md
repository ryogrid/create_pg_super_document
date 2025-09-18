# SnapBuildGetTwoPhaseAt

## Location
src/backend/replication/logical/snapbuild.c: 425 - 433

## Overview
SnapBuildGetTwoPhaseAt returns the LSN at which two-phase commit decoding was first enabled for the snapshot builder, providing information about when two-phase transaction support became active.

## Definition
```c
XLogRecPtr SnapBuildGetTwoPhaseAt(SnapBuild *builder)
```

## Detailed Description
This function serves as an accessor method that returns the LSN (Log Sequence Number) at which two-phase commit decoding was first enabled for a given snapshot builder. Two-phase commit support in logical replication allows for the proper handling of prepared transactions that may span multiple database connections or nodes. The LSN returned by this function marks the point in the WAL stream from which two-phase transactions should be decoded and processed. This information is crucial for determining whether a particular transaction should be handled as a two-phase commit based on when it occurred relative to the enablement of two-phase support.

## Parameters / Member Variables
- `builder`: Pointer to the SnapBuild structure from which to retrieve the two-phase LSN

## Dependencies
- Functions called/Symbols referenced:
  - SnapBuild (structure access)
- Called from (representative examples):
  - DecodeCommit

## Notes and Other Information
This is a simple getter function that directly returns the two_phase_at field from the SnapBuild structure. The field is set during snapshot builder initialization in AllocateSnapshotBuilder. The LSN value is used primarily during commit decoding to determine whether a transaction should be processed with two-phase commit semantics. If the transaction's LSN is at or after the two_phase_at LSN, it indicates that two-phase commit processing should be applied. This mechanism ensures backward compatibility while enabling two-phase commit features from a specific point in the replication stream.