# DomainConstraintRef

## Location
src/include/utils/typcache.h: 164 - 174

## Overview
DomainConstraintRef is a structure that maintains a long-lived reference to a domain type's constraint set, providing memory management and constraint state tracking for domain validation operations.

## Definition


## Detailed Description
DomainConstraintRef serves as a reference-counted wrapper for maintaining access to domain constraint information over extended periods. This structure is essential for PostgreSQL's domain type system, where constraints must be validated whenever values of the domain type are processed.

The structure manages both the constraint list itself and the memory context in which it resides, ensuring proper cleanup when the reference is no longer needed. It works in conjunction with the type cache system to provide efficient access to domain constraints while maintaining consistency when constraints are modified.

The design follows a reference-counting pattern where multiple parts of the system can hold references to the same constraint set, and the constraints are automatically cleaned up when the last reference is released through the callback mechanism.

## Parameters / Member Variables
### Public Interface Fields
- : List of DomainConstraintState nodes representing the actual constraint expressions that need to be evaluated
- : Memory context that holds the DomainConstraintRef structure itself, used for proper memory management
- : Pointer to the TypeCacheEntry for the domain type, providing access to cached type information
- : Boolean flag indicating whether the caller requires executable expression state for constraint checking

### Private Management Fields
- : Pointer to the current DomainConstraintCache containing the constraint definitions, or NULL if no constraints exist
- : Memory context callback structure used to automatically release reference counts when the context is destroyed

## Dependencies
- Functions called/Symbols referenced:
  - [DomainConstraintCache](DomainConstraintCache.md)
  - [MemoryContextCallback](../M/MemoryContextCallback.md)
  - [TypeCacheEntry](../T/TypeCacheEntry.md) (indirectly through tcache field)
  - [List](../L/List.md) (for constraints field)
- Called from (representative examples):
  - [ExecInitCoerceToDomain](../E/ExecInitCoerceToDomain.md) (src/backend/executor/execExpr.c:3349)
  - [DomainIOData](DomainIOData.md) (src/backend/utils/adt/domains.c:59)
  - [InitDomainConstraintRef](../I/InitDomainConstraintRef.md) (src/backend/utils/cache/typcache.c:1313)
  - [UpdateDomainConstraintRef](../U/UpdateDomainConstraintRef.md) (src/backend/utils/cache/typcache.c:1351)
  - [dccref_deletion_callback](../d/dccref_deletion_callback.md) (src/backend/utils/cache/typcache.c:1256)

## Notes and Other Information
- This structure is specifically designed for callers who need to maintain long-lived references to domain constraints
- The management fields (dcc and callback) are private to typcache.c and should not be accessed directly by other modules
- Use InitDomainConstraintRef() to initialize and UpdateDomainConstraintRef() to maintain the reference
- The callback mechanism ensures automatic cleanup when the memory context is destroyed, preventing memory leaks
- [DomainConstraintState](DomainConstraintState.md) nodes are considered executable expressions and are defined in execnodes.h
- The reference counting mechanism allows multiple parts of the system to safely share access to the same constraint information
- Memory management is critical: the refctx field tracks the context containing this structure for proper cleanup coordination