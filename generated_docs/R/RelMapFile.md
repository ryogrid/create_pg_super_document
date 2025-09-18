# RelMapFile

## Location
src/backend/utils/cache/relmapper.c: 89 - 95

## Overview
RelMapFile represents the complete structure of a relation mapping file, containing a collection of RelMapping entries along with metadata for validation and integrity checking.

## Definition
```c
typedef struct RelMapFile
{
    int32       magic;              /* always RELMAPPER_FILEMAGIC */
    int32       num_mappings;       /* number of valid RelMapping entries */
    RelMapping  mappings[MAX_MAPPINGS];
    pg_crc32c   crc;                /* CRC of all above */
} RelMapFile;
```

## Detailed Description
RelMapFile represents the complete in-memory and on-disk structure of a relation mapping file. This structure contains all the information needed to map system catalog OIDs to their corresponding relation file numbers. The structure includes integrity checking mechanisms through a magic number and CRC validation to ensure data consistency.

The file format is designed to be both memory-efficient and disk-friendly, allowing the same structure to be used for both runtime operations and persistent storage. Each RelMapFile can contain up to MAX_MAPPINGS relation mappings, providing a fixed upper bound on the number of system catalogs that can be mapped.

The structure is critical for PostgreSQL's bootstrap process and system catalog management, as it provides the essential mapping information needed to locate physical files for key system catalogs before the regular catalog system is fully operational.

## Parameters / Member Variables
- `magic`: A magic number constant (RELMAPPER_FILEMAGIC) used to validate file format and detect corruption
- `num_mappings`: The count of valid RelMapping entries currently stored in the mappings array
- `mappings[MAX_MAPPINGS]`: Fixed-size array containing the actual RelMapping structures that define OID-to-file-number mappings
- `crc`: A CRC32C checksum calculated over all preceding fields to ensure data integrity

## Dependencies
- Functions called/Symbols referenced:
  - [RelMapping](RelMapping.md) (struct type for array elements)
  - MAX_MAPPINGS (constant defining maximum number of mappings)
  - pg_crc32c (data type for checksum)
- Called from (representative examples):
  - [RelationMapOidToFilenumber](RelationMapOidToFilenumber.md)
  - [RelationMapFilenumberToOid](RelationMapFilenumberToOid.md)
  - [read_relmap_file](../r/read_relmap_file.md)
  - [write_relmap_file](../w/write_relmap_file.md)
  - [perform_relmap_update](../p/perform_relmap_update.md)

## Notes and Other Information
- This structure serves as both the in-memory representation and the on-disk format for relation mapping files
- The magic number and CRC provide robust protection against file corruption
- The fixed-size array design ensures predictable memory usage and file sizes
- Used extensively throughout the relation mapping subsystem for reading, writing, and manipulating mapping data
- Critical for system startup and recovery operations where catalog file locations must be known before the catalog system is fully initialized