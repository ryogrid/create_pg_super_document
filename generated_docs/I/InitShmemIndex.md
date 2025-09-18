# InitShmemIndex

## Location
src/backend/storage/ipc/shmem.c: 283 - 331

## Overview
InitShmemIndex initializes the shared memory index hash table that tracks all named shared memory structures allocated within the PostgreSQL shared memory segment.

## Definition


## Detailed Description
This function creates and initializes the ShmemIndex hash table, which serves as a registry for all named shared memory structures in PostgreSQL. The ShmemIndex acts as a lookup table that maps structure names to their locations in shared memory, enabling other processes to find and attach to previously allocated shared memory structures.

The function faces a bootstrapping challenge: ShmemInitHash internally calls ShmemInitStruct, which expects ShmemIndex to already exist. This circularity is resolved by using the special name "ShmemIndex" - when ShmemInitStruct encounters this specific name, it bypasses the normal lookup mechanism and handles the initialization directly.

The hash table is configured with:
- Key size defined by SHMEM_INDEX_KEYSIZE
- Entry size set to sizeof(ShmemIndexEnt)
- Initial and maximum size set to SHMEM_INDEX_SIZE
- Hash flags HASH_ELEM | HASH_STRINGS for element-based hashing with string keys

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - ShmemInitHash (creates the hash table)
  - HASHCTL (hash table control structure)
  - SHMEM_INDEX_KEYSIZE (macro defining key size)
  - ShmemIndexEnt (structure type for hash table entries)
  - SHMEM_INDEX_SIZE (macro defining table size)
  - HASH_ELEM (hash table flag)
  - HASH_STRINGS (hash table flag)
- Called from (representative examples):
  - [CreateOrAttachShmemStructs](../C/CreateOrAttachShmemStructs.md)

## Notes and Other Information
- This function must be called early in shared memory initialization before other shared memory structures are created
- The circular dependency with ShmemInitStruct is resolved through special handling of the "ShmemIndex" name
- Once initialized, ShmemIndex becomes the central registry for all subsequent shared memory allocations
- The hash table uses string-based keys to identify shared memory structures by name
- The function assumes shared memory segment is already established and basic memory management is available