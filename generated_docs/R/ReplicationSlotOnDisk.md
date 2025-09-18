# ReplicationSlotOnDisk

## Location
src/backend/replication/slot.c: 64 - 82

## Overview
ReplicationSlotOnDisk is a structure that defines the on-disk data format for replication slots in PostgreSQL, providing a versioned and checksummed layout for persistent storage.

## Definition


## Detailed Description
ReplicationSlotOnDisk represents the binary format used to store replication slot information on disk. This structure is carefully designed with version independence in mind, allowing PostgreSQL to handle different versions of slot data formats. The structure includes integrity checking through a CRC32c checksum and magic number validation. The design separates non-checksummed metadata (magic and checksum) from checksummed data (version, length, and actual slot data) to ensure data integrity while allowing for format evolution.

## Parameters / Member Variables
- : Magic number used for file format identification and validation
- : CRC32c checksum covering the versioned data portion for integrity verification
- : Format version number allowing for backward compatibility and format evolution
- : Size of the data structure, enabling proper reading of variable-sized data
- : The actual persistent replication slot data containing slot-specific information

## Dependencies
- Functions called/Symbols referenced:
  - pg_crc32c
  - ReplicationSlotPersistentData
- Called from (representative examples):
  - ReplicationSlotOnDiskConstantSize
  - ReplicationSlotOnDiskNotChecksummedSize 
  - ReplicationSlotOnDiskChecksummedSize
  - SaveSlotToPath
  - RestoreSlotFromDisk

## Notes and Other Information
The structure is designed with version independence as a key principle, with the first part being stable across versions. The separation of checksummed and non-checksummed data allows for efficient integrity verification. The variable-length design accommodates future extensions to the slot data format while maintaining backward compatibility. This structure is fundamental to PostgreSQL's replication slot persistence mechanism, ensuring reliable storage and retrieval of replication state across server restarts.