# BloomOpaque

## Location
src/backend/access/brin/brin_bloom.c: 434 - 442

## Overview
BloomOpaque is a structure that stores cached function manager information for BRIN bloom filter operator class procedures, maintaining consistency with other BRIN operator classes.

## Definition
```c
typedef struct BloomOpaque
{
    /*
     * XXX At this point we only need a single proc (to compute the hash), but
     * let's keep the array just like inclusion and minmax opclasses, for
     * consistency. We may need additional procs in the future.
     */
    FmgrInfo    extra_procinfos[BLOOM_MAX_PROCNUMS];
} BloomOpaque;
```

## Detailed Description
BloomOpaque serves as a cache for function manager information (FmgrInfo) related to BRIN bloom filter operator class procedures. Currently, it primarily stores information for hash computation procedures, but the array structure is designed for extensibility and consistency with other BRIN operator classes (inclusion and minmax). This design anticipates future expansion to support additional procedures while maintaining a uniform interface across different BRIN operator class implementations.

## Parameters / Member Variables
- `extra_procinfos`: Array of FmgrInfo structures that cache function call information for operator class procedures, sized by BLOOM_MAX_PROCNUMS (currently 1)

## Dependencies
- Functions called/Symbols referenced:
  - BLOOM_MAX_PROCNUMS (defined as 1)
- Called from (representative examples):
  - brin_bloom_opcinfo
  - bloom_get_procinfo

## Notes and Other Information
Located in src/backend/access/brin/brin_bloom.c:434-442. The structure follows PostgreSQL's pattern for operator class opaque data, providing a consistent interface for procedure caching across different BRIN operator class types. The current implementation focuses on hash computation but is designed for future expansion to additional procedures as needed.