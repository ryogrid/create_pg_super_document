# SnapBuildSetTwoPhaseAt

## Location
[src/backend/replication/logical/snapbuild.c:434-442](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/snapbuild.c#L434-L442)

## Overview
Sets the LSN at which two-phase decoding is enabled for a snapshot builder, controlling when two-phase commit protocol operations should be decoded during logical replication.

## Definition
```c
void SnapBuildSetTwoPhaseAt(SnapBuild *builder, XLogRecPtr ptr)
```

## Detailed Description
This function is used to configure the LSN (Log Sequence Number) threshold at which two-phase decoding becomes active in a snapshot builder. Two-phase decoding is part of PostgreSQL's logical replication system that handles prepared transactions (two-phase commit protocol). The function simply stores the provided LSN in the builder's `two_phase_at` field, which is later used to determine whether to decode two-phase operations encountered during WAL processing.

## Parameters / Member Variables
- `builder`: Pointer to the SnapBuild structure that manages snapshot building state
- `ptr`: The XLogRecPtr (LSN) at which two-phase decoding should be enabled

## Dependencies
- Functions called/Symbols referenced:
  - [SnapBuild](SnapBuild.md) (struct type)
- Called from (representative examples):
  - [CreateDecodingContext](../C/CreateDecodingContext.md)

## Notes and Other Information
- This is a simple setter function that enables two-phase commit support in logical replication
- The LSN threshold helps optimize performance by only decoding two-phase operations when necessary
- Part of PostgreSQL's logical replication and snapshot building infrastructure
- Located in src/backend/replication/logical/snapbuild.c:434-442