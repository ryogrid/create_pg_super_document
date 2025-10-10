# SnapBuildSerializationPoint

## Location
[src/backend/replication/logical/snapbuild.c:1656-1668](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/snapbuild.c#L1656-L1668)

## Overview
SnapBuildSerializationPoint is a dispatcher function that determines whether to serialize or restore a snapshot based on the snapshot builder's current state.

## Definition
```c
void SnapBuildSerializationPoint(SnapBuild *builder, XLogRecPtr lsn)
```

## Detailed Description
This function serves as a central point for handling snapshot serialization at specific LSN locations during logical replication. It examines the snapshot builder's state and decides whether to:
- Restore a previously serialized snapshot (if the builder state is before SNAPBUILD_CONSISTENT)
- Serialize the current snapshot to disk (if the builder state is SNAPBUILD_CONSISTENT or later)

The function is designed to be called by external code (outside of snapbuild.c) when encountering a record that represents a potential location for a serialized snapshot during WAL replay.

## Parameters / Member Variables
- `builder`: Pointer to the SnapBuild structure containing the snapshot builder state
- `lsn`: XLog record pointer indicating the WAL position where serialization should occur

## Dependencies
- Functions called/Symbols referenced:
  - [SnapBuildRestore](SnapBuildRestore.md)
  - [SnapBuildSerialize](SnapBuildSerialize.md)
  - SNAPBUILD_CONSISTENT (enum constant)
- Called from (representative examples):
  - [xlog_decode](../x/xlog_decode.md) (in decode.c:142)

## Notes and Other Information
- This function acts as a state-based dispatcher, making the decision between restoration and serialization transparent to the caller
- The SNAPBUILD_CONSISTENT state serves as the threshold: before this state, snapshots are restored; at or after this state, snapshots are serialized
- Intended for use by logical decoding infrastructure when processing WAL records that may contain snapshot serialization points

## Simplified Source

```c
void SnapBuildSerializationPoint(SnapBuild *builder, XLogRecPtr lsn)
{
    // Decide whether to restore or serialize based on builder state
    if (builder->state < SNAPBUILD_CONSISTENT)
        SnapBuildRestore(builder, lsn);
    else
        SnapBuildSerialize(builder, lsn);
}
```