# _LoadLOs

## Location
[src/bin/pg_dump/pg_backup_custom.c:580-604](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_custom.c#L580-L604)

## Overview
This function loads (restores) large objects from a PostgreSQL archive by iterating through all large object entries and restoring each one individually.

## Definition
```c
static void _LoadLOs(ArchiveHandle *AH, bool drop)
```

## Detailed Description
_LoadLOs is responsible for restoring all large objects from a PostgreSQL archive during the restoration process. It implements a simple but effective protocol: it reads object identifiers (OIDs) from the archive stream, and for each non-zero OID, it performs a complete restore cycle including initialization, data restoration, and cleanup. The function continues processing until it encounters a zero OID, which serves as an end-of-large-objects marker (as written by _EndLOs during dump creation). The function coordinates with the archive's large object restoration framework to handle the complete restoration lifecycle.

## Parameters / Member Variables
- `AH`: Archive handle containing the file stream and restoration context
- `drop`: Boolean flag indicating whether existing large objects should be dropped before restoration

## Dependencies
- Functions called/Symbols referenced:
  - [StartRestoreLOs](../S/StartRestoreLOs.md)
  - [ReadInt](../R/ReadInt.md)
  - [StartRestoreLO](../S/StartRestoreLO.md)
  - [_PrintData](../P/_PrintData.md)
  - [EndRestoreLO](../E/EndRestoreLO.md)
  - [EndRestoreLOs](../E/EndRestoreLOs.md)
  - Oid (type)
- Called from (representative examples):
  - [_PrintTocData](../P/_PrintTocData.md) (in custom format when processing BLK_BLOBS)
  - [_PrintTocData](../P/_PrintTocData.md) (in directory format)
  - [_PrintTocData](../P/_PrintTocData.md) (in tar format)

## Notes and Other Information
This function relies on the zero OID terminator protocol established by _EndLOs during the dump process. The drop parameter allows for different restoration strategies - [when](../w/when.md) true, existing large objects with the same OID will be removed before restoring the archived version. The function uses _PrintData to handle the actual data restoration, ensuring consistent handling of compressed data streams.