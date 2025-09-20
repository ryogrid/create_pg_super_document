# test_dsa_resowners

## Location
[src/test/modules/test_dsa/test_dsa.c:64-113](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_dsa/test_dsa.c#L64-L113)

## Overview
A PostgreSQL test function that validates Dynamic Shared Area (DSA) integration with the resource ownership system, testing automatic cleanup of DSA allocations when resource owners are released.

## Definition
Datum test_dsa_resowners(PG_FUNCTION_ARGS)

## Detailed Description
This function tests the interaction between PostgreSQL's resource ownership system and DSA memory management. It creates a DSA area in the parent resource owner context, then switches to a child resource owner to perform 10,000 memory allocations within the DSA. The function tests both allocation and partial deallocation (freeing 500 out of 10,000 blocks), then triggers resource owner cleanup to verify that remaining DSA allocations are properly tracked and cleaned up automatically when the child resource owner is released. This ensures that DSA integrates correctly with PostgreSQL's resource management framework to prevent memory leaks.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - LWLockNewTrancheId: Creates a new lightweight lock tranche identifier
  - LWLockRegisterTranche: Registers the tranche with a descriptive name
  - dsa_create: Creates a new DSA area
  - ResourceOwnerCreate: Creates a child resource owner
  - dsa_allocate: Allocates memory blocks within the DSA
  - [dsa_get_address](../d/dsa_get_address.md): Translates DSA pointers to virtual addresses
  - [dsa_free](../d/dsa_free.md): Frees allocated memory blocks
  - ResourceOwnerRelease: Releases resources managed by a resource owner
  - [ResourceOwnerDelete](../R/ResourceOwnerDelete.md): Deletes a resource owner
  - dsa_detach: Detaches from and cleans up the DSA area
  - PG_RETURN_VOID: PostgreSQL macro for returning void from functions
- Called from (representative examples):
  - [test_dsa_basic](test_dsa_basic.md): Referenced in the same file (cross-reference)

## Notes and Other Information
- Tests resource ownership integration with a much larger allocation count (10,000) compared to test_dsa_basic (100)
- Creates a temporary child resource owner named "test_dsa temp owner" for isolation testing
- Performs three-phase resource release following PostgreSQL's standard cleanup protocol:
  - RESOURCE_RELEASE_BEFORE_LOCKS: Release resources before releasing locks
  - RESOURCE_RELEASE_LOCKS: Release locks themselves
  - RESOURCE_RELEASE_AFTER_LOCKS: Final cleanup after lock release
- Partially frees allocations (500 out of 10,000) to test mixed scenarios
- The function creates a tranche for lightweight locks but does not properly clean it up (marked with XXX comment)
- Located in src/test/modules/test_dsa/test_dsa.c:64-113
- This is a testing utility function specifically for validating resource management integration