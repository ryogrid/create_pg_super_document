# rtypalign

## Location
[src/interfaces/ecpg/compatlib/informix.c:995-1002](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/compatlib/informix.c#L995-L1002)

## Overview
The rtypalign function is a stub implementation that provides Informix compatibility for type alignment calculations, but currently returns a no-op result.

## Definition

```c
int
rtypalign(int offset, int type)
```
## Detailed Description
The rtypalign function is part of PostgreSQL's ECPG Informix compatibility library. This is a stub implementation that mimics the signature of the Informix rtypalign function, which would typically calculate memory alignment requirements for different data types at a given offset. However, the PostgreSQL implementation simply ignores all parameters and returns 0, indicating no alignment adjustment.

In a full implementation, this function would determine the proper memory alignment boundary for a specific data type when placed at a given offset, which is crucial for efficient memory access and avoiding alignment faults on certain architectures.

## Parameters / Member Variables
- `offset`: Current memory offset position (ignored in current implementation)
- `type`: Data type identifier for alignment calculation (ignored in current implementation)
## Dependencies
- Functions called/Symbols referenced:
  - None (only uses void casts for compiler quieting)
- Called from (representative examples):
  - ECPG_INFORMIX_EXTRA_CHARS macro context

## Notes and Other Information
- This is a stub implementation - no actual alignment calculation occurs
- Always returns 0, indicating no alignment adjustment needed
- All parameters are intentionally ignored with void casts to suppress compiler warnings
- Provides API compatibility with Informix applications that use rtypalign()
- In a real implementation, would handle architecture-specific alignment requirements
- Located in src/interfaces/ecpg/compatlib/informix.c:995-1002
- May require actual implementation for applications that depend on proper type alignment

## Simplified Source

```c
int rtypalign(int offset, int type) {
    // Stub implementation: ignore parameters to suppress compiler warnings
    (void) offset;
    (void) type;

    // Always return 0 (no alignment adjustment)
    return 0;
}
```