# BlockRefTableSerializedEntry

## Location
[src/common/blkreftable.c:155-161](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/blkreftable.c#L155-L161)

## Overview
BlockRefTableSerializedEntry defines the on-disk serialization format for block reference table entries, providing a compact representation for persistent storage and transmission.

## Definition
```c
typedef struct BlockRefTableSerializedEntry
{
    RelFileLocator rlocator;
    ForkNumber     forknum;
    BlockNumber    limit_block;
    uint32         nchunks;
} BlockRefTableSerializedEntry;
```

## Detailed Description
BlockRefTableSerializedEntry represents the standardized on-disk format used for serializing block reference table entries. This structure contains the essential metadata needed to identify a relation fork and its block tracking state without the complex in-memory chunk management structures. It serves as an intermediate format during serialization and deserialization operations, enabling efficient storage and retrieval of block reference information. The structure includes only the core identifying information and summary statistics, with the detailed chunk data serialized separately.

## Parameters / Member Variables
- `rlocator`: RelFileLocator that identifies the specific relation file, containing database, tablespace, and relation OID information
- `forknum`: ForkNumber specifying which fork of the relation this entry represents (e.g., MAIN_FORKNUM, FSM_FORKNUM, VISIBILITYMAP_FORKNUM)
- `limit_block`: The shortest known length of the relation in blocks; represents the relation size within the tracked LSN range
- `nchunks`: The number of chunks used to represent block modification data for this relation fork

## Dependencies
- Functions called/Symbols referenced: None (structure definition only)
- Used by:
  - [WriteBlockRefTable](../W/WriteBlockRefTable.md) (multiple references for serialization operations)
  - [BlockRefTableReaderNextRelation](BlockRefTableReaderNextRelation.md) (multiple references for deserialization)
  - [BlockRefTableWriteEntry](BlockRefTableWriteEntry.md) (for writing serialized entries)
  - [BlockRefTableComparator](BlockRefTableComparator.md) (for comparing serialized entries)
  - [BlockRefTableFileTerminate](BlockRefTableFileTerminate.md) (for finalizing serialized files)

## Notes and Other Information
- Defined in src/common/blkreftable.c:155-161 with documentation at lines 152-154
- Serves as the persistent storage format for block reference table data
- Contains only essential metadata; detailed chunk data is serialized separately following this header
- Used extensively in serialization/deserialization workflows for block reference tables
- Enables efficient disk storage and network transmission of block reference information
- The structure is designed to be platform-independent for cross-system compatibility
- Critical component for incremental backup file formats and WAL summary serialization