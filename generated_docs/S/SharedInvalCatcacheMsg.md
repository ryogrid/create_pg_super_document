# SharedInvalCatcacheMsg

## Location
src/include/storage/sinval.h: 65 - 66

## Overview
SharedInvalCatcacheMsg is a structure that represents a shared invalidation message for invalidating a specific tuple in a specific catcache (catalog cache) across PostgreSQL processes.

## Definition


## Detailed Description
SharedInvalCatcacheMsg is one of several shared invalidation message types used in PostgreSQL's cache invalidation system. It specifically handles invalidation of individual catalog cache entries by identifying them through a cache ID, database ID, and hash value of the cached key. This allows precise invalidation of specific cached tuples rather than invalidating entire caches, which improves performance by avoiding unnecessary cache rebuilds.

The structure is part of PostgreSQL's shared invalidation mechanism that ensures cache consistency across multiple backend processes when database metadata changes. When a catalog tuple is modified, an invalidation message is sent to all processes to remove the corresponding cached entry.

## Parameters / Member Variables
- uid=1000(ryo) gid=1000(ryo) groups=1000(ryo),4(adm),20(dialout),24(cdrom),25(floppy),27(sudo),29(audio),30(dip),44(video),46(plugdev),117(netdev),998(ollama),999(docker): Cache ID that identifies which catalog cache this invalidation applies to (must be the first field for message type identification)
- : Database ID for database-specific catalogs, or 0 for shared system catalogs that apply to all databases
- : Hash value of the key for the specific catalog cache entry to be invalidated, allowing precise identification of the cached tuple

## Dependencies
- Functions called/Symbols referenced:
  - Oid (PostgreSQL object identifier type)
  - int8, uint32 (PostgreSQL integer types)
- Called from (representative examples):
  - SharedInvalidationMessage (union containing this structure)
  - Various cache invalidation functions in the sinval subsystem

## Notes and Other Information
- The uid=1000(ryo) gid=1000(ryo) groups=1000(ryo),4(adm),20(dialout),24(cdrom),25(floppy),27(sudo),29(audio),30(dip),44(video),46(plugdev),117(netdev),998(ollama),999(docker) field must be the first member of the structure to allow the message type to be determined by examining the first byte
- This structure is part of a union (SharedInvalidationMessage) that encompasses all types of invalidation messages
- The hash value mechanism allows for efficient identification of specific cached entries without needing to store the full key
- Used in PostgreSQL's shared memory-based invalidation system to maintain cache coherence across multiple processes
- Database ID of 0 indicates the invalidation applies to shared system catalogs that are visible across all databases