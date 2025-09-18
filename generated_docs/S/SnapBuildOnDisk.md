# SnapBuildOnDisk

## Location
[src/backend/replication/logical/snapbuild.c:1620-1639](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/snapbuild.c#L1620-L1639)

## Overview
SnapBuildOnDisk is a serialization structure used to persist SnapBuild snapshot state to disk, enabling snapshot restoration across PostgreSQL restarts and supporting consistent logical replication recovery.

## Definition


## Detailed Description
SnapBuildOnDisk provides a persistent storage format for SnapBuild structures, allowing logical replication slots to maintain consistency across database restarts and recovery scenarios. The structure is carefully designed with version independence and data integrity in mind.

The serialization format stores the complete SnapBuild state including all committed transaction arrays and catalog change tracking information. The on-disk layout consists of:
1. Fixed header with magic number and checksum for integrity verification
2. Version and length information for forward/backward compatibility
3. Complete SnapBuild structure
4. Variable-length arrays of TransactionIds for committed and catalog-change transactions

The structure uses magic number verification (SNAPBUILD_MAGIC = 0x51A1E001) and CRC32c checksums to detect corruption. Version information (SNAPBUILD_VERSION = 6) allows for future schema evolution while maintaining compatibility.

## Parameters / Member Variables
- : Magic number (0x51A1E001) for file format identification and corruption detection
- : CRC32c checksum covering all data after this field for integrity verification
- : Format version number (currently 6) enabling pg_upgrade compatibility and schema evolution
- : Size of variable-length data portion, excluding the fixed-size header
- : Complete embedded SnapBuild structure containing all snapshot state
- : Following the structure, variable-length arrays containing:
  - committed.xcnt TransactionIds from the committed transactions array
  - catchange.xcnt TransactionIds from the catalog changes array

## Dependencies
- Functions called/Symbols referenced:
  - pg_crc32c (checksum calculation for data integrity)
  - [SnapBuild](SnapBuild.md) (embedded snapshot builder state)
  - TransactionId arrays (variable-length transaction lists)

- Called from (representative examples):
  - [SnapBuildSerialize](SnapBuildSerialize.md) (creates and writes SnapBuildOnDisk to disk)
  - [SnapBuildRestore](SnapBuildRestore.md) (reads and validates SnapBuildOnDisk from disk)
  - SnapBuildOnDiskConstantSize (macro calculating fixed header size)
  - SnapBuildOnDiskNotChecksummedSize (macro for checksum boundary calculation)

## Notes and Other Information
- The structure layout is carefully ordered with version-independent data first to support future schema changes
- Checksum covers all data except the magic number and checksum field itself
- [Variable](../V/Variable.md)-length TransactionId arrays are stored immediately after the structure in a specific order: committed transactions first, then catalog change transactions
- The magic number 0x51A1E001 helps identify valid snapshot files and detect corruption
- Current version 6 indicates the format has evolved over PostgreSQL development history
- Used primarily during logical replication slot initialization and recovery scenarios
- The constant size macros (SnapBuildOnDiskConstantSize, SnapBuildOnDiskNotChecksummedSize) help with serialization calculations
- File I/O operations use temporary files and atomic renames to ensure consistency during writes