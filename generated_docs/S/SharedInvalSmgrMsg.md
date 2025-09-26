# SharedInvalSmgrMsg

## Location
src/include/storage/sinval.h: 94 - 95

## Overview
SharedInvalSmgrMsg is a structure that represents a shared invalidation message for invalidating smgr (storage manager) cache entries for specific physical relations across PostgreSQL processes.

## Definition


## Detailed Description
SharedInvalSmgrMsg is part of PostgreSQL's shared invalidation system, specifically designed to handle invalidation of storage manager (smgr) cache entries. The smgr cache stores information about physical relation files on disk, including file handles and metadata. This structure is optimized for space efficiency, packing into exactly 16 bytes.

The structure supports invalidation of both regular relations and temporary relations. For temporary relations, it includes backend process identification through the backend_hi and backend_lo fields, since temporary relations are backend-specific. The RelFileLocator uniquely identifies the physical relation file that needs cache invalidation.

## Parameters / Member Variables
- uid=1000(ryo) gid=1000(ryo) groups=1000(ryo),4(adm),20(dialout),24(cdrom),25(floppy),27(sudo),29(audio),30(dip),44(video),46(plugdev),117(netdev),998(ollama),999(docker): Type field that must be the first member to identify this as an smgr invalidation message (set to SHAREDINVALSMGR_ID which is -3)
- : High 8 bits of the backend process number, used only for temporary relations to identify the owning backend process
- : Low 16 bits of the backend process number, used only for temporary relations to identify the owning backend process
- : RelFileLocator structure containing tablespace OID (spcOid), database OID (dbOid), and relation file number (relNumber) to uniquely identify the physical relation file

## Dependencies
- Functions called/Symbols referenced:
  - RelFileLocator (structure for identifying relation files)
  - int8, uint16 (PostgreSQL integer types)
  - SHAREDINVALSMGR_ID (constant defined as -3)
- Called from (representative examples):
  - SharedInvalidationMessage (union containing this structure)
  - Storage manager invalidation functions in the sinval subsystem

## Notes and Other Information
- The uid=1000(ryo) gid=1000(ryo) groups=1000(ryo),4(adm),20(dialout),24(cdrom),25(floppy),27(sudo),29(audio),30(dip),44(video),46(plugdev),117(netdev),998(ollama),999(docker) field is set to SHAREDINVALSMGR_ID (-3) to distinguish smgr invalidation messages from other message types
- Structure layout is carefully designed to pack into exactly 16 bytes for memory efficiency
- For non-temporary relations, the backend_hi and backend_lo fields are not used
- For temporary relations, backend_hi and backend_lo combine to form a full backend process number (backend_hi << 16 | backend_lo)
- Used when physical relation files are created, dropped, or their metadata changes
- Part of the SharedInvalidationMessage union that encompasses all invalidation message types
- Critical for maintaining consistency of storage manager cache across multiple PostgreSQL backend processes
- Handles invalidation at the physical storage level rather than logical relation level