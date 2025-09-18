# RecordCacheArrayEntry

## Location
src/backend/utils/cache/typcache.c: 278 - 282

## Overview
RecordCacheArrayEntry is a structure used in PostgreSQL's type cache system to store mappings between unique identifiers and their corresponding TupleDesc structures in a local backend array cache.

## Definition


## Detailed Description
RecordCacheArrayEntry serves as the fundamental building block for PostgreSQL's local record cache array (RecordCacheArray). This structure provides a local backend-specific cache for record type information, storing mappings between unique 64-bit identifiers and their corresponding TupleDesc structures. Unlike the shared memory structures, this cache is specific to each backend process and provides fast access to frequently used record type descriptors.

The structure is used within an expandable array that grows dynamically as needed to accommodate new typmod values. The cache helps optimize performance by avoiding repeated tuple descriptor lookups and reconstructions for commonly used record types within a single backend session.

## Parameters / Member Variables
- uid=1000(ryo) gid=1000(ryo) groups=1000(ryo),4(adm),20(dialout),24(cdrom),25(floppy),27(sudo),29(audio),30(dip),44(video),46(plugdev),117(netdev),998(ollama),999(docker): A 64-bit unsigned integer serving as a unique identifier for the cached record type
- : A pointer to a TupleDesc structure containing the complete tuple descriptor information for the record type

## Dependencies
- Functions called/Symbols referenced:
  - TupleDesc (implicitly referenced)
- Called from (representative examples):
  - ensure_record_cache_typmod_slot_exists

## Notes and Other Information
- Located in src/backend/utils/cache/typcache.c:278-282
- Used as the element type for the RecordCacheArray global variable
- Part of the local backend cache system complementing the shared memory record cache
- The array starts with an initial size of 64 entries and grows using powers of 2 as needed
- Provides O(1) access time for record type lookups when the typmod is known
- Backend-local storage means each PostgreSQL process maintains its own copy of this cache