# DSMRegistryEntry

## Location
[src/backend/storage/ipc/dsm_registry.c:43-48](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/dsm_registry.c#L43-L48)

## Overview
DSMRegistryEntry represents an individual entry in the dynamic shared memory registry, storing the name, handle, and size information for a named DSM segment.

## Definition
```c
typedef struct DSMRegistryEntry
{
    char        name[64];
    dsm_handle  handle;
    size_t      size;
} DSMRegistryEntry;
```

## Detailed Description
This structure defines the format of entries stored in the dynamic shared memory registry hash table. Each entry maps a string-based name (up to 63 characters plus null terminator) to a specific DSM segment identified by its handle and size. The registry allows libraries to create and access shared memory segments by name without requiring pre-allocation at startup time.

DSMRegistryEntry structures are stored in a distributed hash table managed by the DSM registry system, enabling multiple PostgreSQL backends to safely access and share named memory segments. The entry serves as the persistent metadata that allows the system to locate and attach to previously created DSM segments.

## Parameters / Member Variables
- `name[64]`: Null-terminated string identifier for the DSM segment, limited to 63 characters plus null terminator
- `handle`: DSM handle that uniquely identifies the dynamic shared memory segment
- `size`: Size in bytes of the associated DSM segment

## Dependencies
- Functions called/Symbols referenced:
  - dsm_handle
- Called from (representative examples):
  - [GetNamedDSMSegment](../G/GetNamedDSMSegment.md)

## Notes and Other Information
- The 64-byte name field limits segment names to 63 characters to ensure null termination
- Entries are stored in a distributed hash table accessible across multiple PostgreSQL backends
- The handle field allows the system to attach to existing DSM segments created by other processes
- Size information is maintained for consistency checking and memory management
- Located in src/backend/storage/ipc/dsm_registry.c at lines 43-48