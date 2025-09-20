# InitDomainConstraintRef

## Location
[src/backend/utils/cache/typcache.c:1313-1350](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/typcache.c#L1313-L1350)

## Overview
Initializes a DomainConstraintRef structure that manages references to domain constraint information with proper memory context management and optional expression state preparation.

## Definition

```c
void
InitDomainConstraintRef(Oid type_id, DomainConstraintRef *ref,
						MemoryContext refctx, bool need_exprstate)
```
## Detailed Description
This function sets up a DomainConstraintRef structure that provides a managed reference to domain constraint information. It handles the complete initialization process including memory context registration, reference counting, and optional expression state preparation. The function establishes a cleanup callback to ensure proper resource management when the memory context is reset or deleted.

The function performs several key operations: it looks up the type cache entry for the specified domain type, sets up memory context callbacks for automatic cleanup, manages reference counting for shared constraint data, and optionally prepares executable expression states for constraint checking. This design allows multiple references to share the same constraint data while maintaining proper lifecycle management.

## Parameters / Member Variables
- : Object identifier of the domain type for which constraints are being referenced
- : Pointer to the DomainConstraintRef structure to be initialized
- : Memory context in which the reference lives and will be cleaned up
- : Boolean flag indicating whether executable expression states should be prepared for constraints

## Dependencies
- Functions called/Symbols referenced:
  - [DomainConstraintRef](../D/DomainConstraintRef.md) (struct for managing constraint references)
  - [lookup_type_cache](../l/lookup_type_cache.md) (retrieves type cache information)
  - TYPECACHE_DOMAIN_CONSTR_INFO (flag for domain constraint information)
  - [dccref_deletion_callback](../d/dccref_deletion_callback.md) (cleanup callback function)
  - [MemoryContextRegisterResetCallback](../M/MemoryContextRegisterResetCallback.md) (registers memory context cleanup)
  - [prep_domain_constraints](../p/prep_domain_constraints.md) (prepares constraints for execution)
- Called from (representative examples):
  - [ExecInitCoerceToDomain](../E/ExecInitCoerceToDomain.md) (executor initialization)
  - [domain_state_setup](../d/domain_state_setup.md) (domain constraint setup)

## Notes and Other Information
- The function registers a deletion callback to ensure proper cleanup of constraint references when the memory context is reset
- Reference counting is used to manage shared constraint data across multiple references
- When need_exprstate is false, the function simply references the cached constraint list without copying
- When need_exprstate is true, it calls prep_domain_constraints to create executable expression states
- The type cache entry is assumed to survive indefinitely, making it safe to hold references to it