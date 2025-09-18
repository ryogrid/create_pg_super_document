# shm_toc_entry

## Location
src/backend/storage/ipc/shm_toc.c: 20 - 24

## Overview
The shm_toc_entry structure represents a single entry in a shared memory table of contents (TOC), storing a key-value mapping where the key is an arbitrary 64-bit identifier and the value is an offset to data within the shared memory segment.

## Definition


## Detailed Description
The shm_toc_entry structure is a fundamental building block of PostgreSQL's shared memory TOC system. Each entry maps a 64-bit key to a byte offset within a shared memory segment. This design allows processes to register and discover data structures within shared memory segments using well-known keys, even when the segment is mapped at different virtual addresses in different processes.

The structure uses relative offsets rather than absolute pointers to ensure portability across different process address spaces. The key is typically a well-known or discoverable integer that uniquely identifies a particular data structure or resource within the shared memory segment.

## Parameters / Member Variables
- : A 64-bit arbitrary identifier used as a unique key for looking up data structures within the shared memory segment
- : The byte offset from the start of the TOC structure to the actual data, stored as a Size type to ensure proper alignment and portability

## Dependencies
- Functions called/Symbols referenced:
  - None (this is a data structure)
- Used by:
  - shm_toc (as array member toc_entry)
  - shm_toc_allocate
  - shm_toc_freespace  
  - shm_toc_insert
  - shm_toc_estimate

## Notes and Other Information
- This structure is designed to be simple and lightweight, as the TOC system is intended for storing only the minimum number of bootstrap pointers needed to access a shared memory segment
- The design explicitly trades off scalability for simplicity - it's not intended to handle large numbers of entries
- Entries are stored in a flexible array within the shm_toc structure, allowing for dynamic sizing based on the number of registered data structures
- Access to the TOC entries is protected by a spinlock (toc_mutex) in the containing shm_toc structure to ensure thread safety during concurrent access