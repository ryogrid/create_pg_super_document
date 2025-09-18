# TwoPhaseRecordOnDisk

## Location
src/backend/access/transam/twophase.c: 983 - 988

## Overview
TwoPhaseRecordOnDisk is a header structure for each record stored in a two-phase commit state file, providing metadata about the resource manager data that follows.

## Definition
```c
typedef struct TwoPhaseRecordOnDisk
{
    uint32          len;        /* length of rmgr data */
    TwoPhaseRmgrId rmid;        /* resource manager for this record */
    uint16          info;       /* flag bits for use by rmgr */
} TwoPhaseRecordOnDisk;
```

## Detailed Description
This structure serves as a record header in two-phase commit state files that are persisted to disk. Each record in the state file begins with this header, followed by the actual resource manager data. The structure provides essential metadata needed to properly interpret and process the resource manager data during recovery or commit/rollback operations.

The design ensures that resource manager data is stored on MAXALIGN boundaries for proper memory alignment, with the len field specifically counting only the resource manager data bytes, excluding the header itself. This separation allows for efficient parsing and processing of state file contents during crash recovery or distributed transaction completion.

## Parameters / Member Variables
- `len`: Length in bytes of the resource manager data that follows this header (does not include the size of the TwoPhaseRecordOnDisk header itself)
- `rmid`: Identifier of the resource manager responsible for this record, determining how the following data should be interpreted and processed
- `info`: Bitfield containing flags and metadata for use by the specific resource manager, providing context for processing the associated data

## Dependencies
- Functions called/Symbols referenced:
  - TwoPhaseRmgrId (enum/type identifying resource managers)
- Called from (representative examples):
  - RegisterTwoPhaseRecord (for creating new records in state files)
  - ReadTwoPhaseFile (for reading and parsing existing state files)
  - ProcessRecords (for processing records during recovery or commit/rollback)

## Notes and Other Information
- The structure is designed for disk persistence and must maintain binary compatibility across PostgreSQL versions
- Resource manager data following this header is aligned on MAXALIGN boundaries for optimal memory access
- The len field specifically excludes the header size to simplify offset calculations when reading sequential records
- This is a critical component of PostgreSQL's crash recovery mechanism for distributed transactions
- The info field allows resource managers to store custom metadata without extending the core header structure