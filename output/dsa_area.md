# dsa_area

## Overview
The dsa_area struct represents the per-backend state for a Dynamic Shared Area (DSA) storage system in PostgreSQL, providing each process with its own mapping and tracking information for accessing shared memory segments. This structure serves as the local interface for backends to interact with DSA shared memory areas, managing segment mappings, resource ownership, and synchronization state. It acts as a critical component in PostgreSQL's parallel processing infrastructure by enabling efficient cross-process memory allocation and deallocation through a sophisticated segment-based mapping system.

## Definition
```c
struct dsa_area
{
    /* Pointer to the control object in shared memory. */
    dsa_area_control *control;

    /*
     * All the mappings are owned by this.  The dsa_area itself is not
     * directly tracked by the ResourceOwner, but the effect is the same. NULL
     * if the attachment has session lifespan, i.e if dsa_pin_mapping() has
     * been called.
     */
    ResourceOwner resowner;

    /*
     * This backend's array of segment maps, ordered by segment index
     * corresponding to control->segment_handles.  Some of the area's segments
     * may not be mapped in this backend yet, and some slots may have been
     * freed and need to be detached; these operations happen on demand.
     */
    dsa_segment_map segment_maps[DSA_MAX_SEGMENTS];

    /* The highest segment index this backend has ever mapped. */
    dsa_segment_index high_segment_index;

    /* The last observed freed_segment_counter. */
    size_t freed_segment_counter;
};
```

## Detailed Description
The dsa_area structure implements PostgreSQL's sophisticated per-process state management for Dynamic Shared Area memory allocation, providing each backend with its own view and control interface to shared memory segments. The structure maintains a pointer to the shared dsa_area_control object that coordinates global state across all processes, while managing local mapping information through an array of segment maps that correspond to actual memory segments. The design enables lazy segment mapping where segments are only mapped into a process's address space when needed, optimizing memory usage and reducing startup overhead. The structure tracks the highest segment index ever mapped by the backend and maintains a freed segment counter to detect when segments have been deallocated by other processes, enabling proper cleanup and consistency. Resource ownership integration through the ResourceOwner system ensures that all mappings are properly cleaned up when transactions abort or sessions end, preventing memory leaks in long-running PostgreSQL processes.

## Parameters / Member Variables
- `control`: Pointer to the dsa_area_control structure in shared memory that contains the global state and coordination information for the DSA area, including segment handles, allocation statistics, and synchronization primitives used across all attached processes
- `resowner`: ResourceOwner that tracks all memory mappings associated with this DSA area, ensuring proper cleanup during transaction abort or session termination; set to NULL when dsa_pin_mapping() is called to indicate session-lifespan attachment
- `segment_maps`: Fixed-size array of dsa_segment_map structures indexed by segment number, providing per-process mapping information for each segment including virtual address, size, and mapping state; segments are mapped on-demand as allocation requests require them
- `high_segment_index`: Tracks the highest segment index that this particular backend has ever mapped, used for optimization during segment iteration and cleanup operations to avoid scanning unmapped segment slots
- `freed_segment_counter`: Local copy of the global freed segment counter used for detecting when other processes have freed segments, triggering this backend to perform cleanup operations and unmap segments that are no longer valid

## Dependencies
- **Functions called/Symbols referenced**:
  - `dsa_area_control` - Global shared memory control structure that coordinates DSA operations across all processes
  - `ResourceOwner` - PostgreSQL's resource management system for tracking and cleaning up allocated resources
  - `dsa_segment_map` - Per-process mapping information for individual memory segments
  - `DSA_MAX_SEGMENTS` - Constant defining the maximum number of segments that can be managed by a single DSA area
  - `dsa_segment_index` - Type definition for segment indexing and identification
- **Called from (representative examples)**:
  - `dsa_create` - Creates a new DSA area and initializes the dsa_area structure for the creating process
  - `dsa_attach` - Attaches an existing process to a DSA area by creating a new dsa_area structure instance
  - `dsa_allocate` - Uses the dsa_area to allocate memory from the shared area, updating segment mappings as needed
  - `dsa_free` - Accesses segment mapping information through the dsa_area to deallocate shared memory

## Notes & Other Information
The dsa_area structure is designed for high-performance parallel workloads where multiple PostgreSQL processes need efficient access to shared memory regions. The lazy mapping approach minimizes memory overhead by only mapping segments that are actually used by each process, which is particularly important in systems with many worker processes. Thread safety is achieved through the shared dsa_area_control structure which contains appropriate locking mechanisms, while the per-process dsa_area structure itself does not require locking since it's only accessed by a single backend. The freed_segment_counter mechanism implements an optimistic approach to detecting segment deallocations, avoiding the need for expensive locking during normal allocation operations. When integrating with PostgreSQL's ResourceOwner system, the structure ensures that all memory mappings are properly tracked and cleaned up even in error conditions, maintaining system stability during complex parallel operations.