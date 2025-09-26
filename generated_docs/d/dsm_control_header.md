# dsm_control_header

## Location
[src/backend/storage/ipc/dsm.c:90-96](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/dsm.c#L90-L96)

## Overview
The header structure that defines the layout of PostgreSQL's dynamic shared memory control segment, containing metadata and an array of control items for managing DSM segments.

## Definition

```c
typedef struct dsm_control_header
{
	uint32		magic;
	uint32		nitems;
	uint32		maxitems;
	dsm_control_item item[FLEXIBLE_ARRAY_MEMBER];
} dsm_control_header;
```
## Detailed Description
The  structure serves as the header for PostgreSQL's DSM control segment, which is a special shared memory segment used to coordinate the lifecycle of all other DSM segments in the system. This structure appears at the beginning of the control segment and is followed by an array of  structures. The control segment acts as a registry where all active DSM segments are tracked, enabling proper cleanup, reference counting, and coordination across multiple backend processes. The magic number provides validation that the control segment is properly initialized and formatted.

## Parameters / Member Variables
- `magic`: A  magic number (value  defined as ) used to validate the integrity and proper initialization of the control segment
- `nitems`: A  value indicating the current number of active DSM segments tracked in the control segment
- `maxitems`: A  value indicating the maximum number of DSM segments that can be tracked in this control segment (determines the size of the item array)
- `item[FLEXIBLE_ARRAY_MEMBER]`: A flexible array member of  structures, each representing the global state of a single DSM segment
## Dependencies
- Functions called/Symbols referenced:
  - [dsm_control_item](dsm_control_item.md) (the structure type used for the flexible array member)
  - FLEXIBLE_ARRAY_MEMBER (macro for defining flexible array members)
  - PG_DYNSHMEM_CONTROL_MAGIC (magic number constant)
- Called from (representative examples):
  - [dsm_cleanup_using_control_segment](dsm_cleanup_using_control_segment.md) (accesses and processes the control header)
  - [dsm_control_segment_sane](dsm_control_segment_sane.md) (validates the control header structure)
  - [dsm_control_bytes_needed](dsm_control_bytes_needed.md) (calculates size requirements including the header)

## Notes and Other Information
- This structure resides at the beginning of the DSM control segment, which is itself a shared memory segment visible to all PostgreSQL backends
- The magic number () serves as a sanity check to ensure the control segment is properly formatted and initialized
- The flexible array member allows the control segment to accommodate a variable number of DSM segment entries based on system configuration
- The maxitems field is typically set during control segment initialization and determines the total capacity for DSM segments
- The nitems field is updated as DSM segments are created and destroyed, providing a current count of active segments
- Access to this structure must be properly synchronized across multiple backends using appropriate locking mechanisms
- The control segment containing this structure is created during PostgreSQL startup and persists for the lifetime of the postmaster process