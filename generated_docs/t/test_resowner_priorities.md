# test_resowner_priorities

## Location
[src/test/modules/test_resowner/test_resowner_basic.c:52-141](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_resowner/test_resowner_basic.c#L52-L141)

## Overview
test_resowner_priorities is a PostgreSQL SQL-callable function that tests the resource owner priority system by creating resources with different release priorities and phases, then demonstrating their ordered cleanup.

## Definition
```c
Datum test_resowner_priorities(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a comprehensive test for PostgreSQL's resource owner priority and release phase system. It creates parent and child resource owners, registers multiple resources with different priority levels and release phases (before locks and after locks), and then demonstrates the ordered release of resources according to their configured priorities. The function serves as both a test case and a demonstration of how the resource owner subsystem manages resource cleanup ordering in PostgreSQL.

The function creates two types of resource descriptors with different release phases, populates both parent and child resource owners with test resources, and then performs a complete resource release cycle that includes before-locks, locks, and after-locks phases while logging each step.

## Parameters / Member Variables
- `nkinds`: Int32 parameter specifying the number of different resource kinds to create (must be > 0)
- `nresources`: Int32 parameter specifying the number of resources to create per kind (must be > 0)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT32 (parameter extraction)
  - [ResourceOwnerCreate](../R/ResourceOwnerCreate.md) (resource owner creation)
  - [ResourceOwnerEnlarge](../R/ResourceOwnerEnlarge.md) (resource owner capacity management)
  - [ResourceOwnerRemember](../R/ResourceOwnerRemember.md) (resource registration)
  - [ResourceOwnerRelease](../R/ResourceOwnerRelease.md) (resource cleanup)
  - [ResourceOwnerDelete](../R/ResourceOwnerDelete.md) (resource owner cleanup)
  - [CStringGetDatum](../C/CStringGetDatum.md) (string to Datum conversion)
  - [ReleaseString](../R/ReleaseString.md) (custom resource release callback)
  - [PrintString](../P/PrintString.md) (custom resource print callback)
  - elog (logging)
  - [palloc](../p/palloc.md) (memory allocation)
  - [psprintf](../p/psprintf.md) (formatted string creation)
- Called from (representative examples):
  - SQL queries invoking this test function

## Notes and Other Information
- Part of the test_resowner module for testing PostgreSQL resource management
- Demonstrates the three-phase resource release process: before locks, locks, and after locks
- Tests both parent-child resource owner relationships and priority-based cleanup ordering
- Uses RELEASE_PRIO_FIRST as the base priority and increments from there
- Validates that nkinds and nresources parameters are positive values
- Provides comprehensive logging of the resource release process for debugging and verification
- Returns void as it is primarily a testing/demonstration function
- The function exercises the complete resource owner lifecycle including creation, population, release, and cleanup

## Simplified Source

```c
// Simplified version of test_resowner_priorities
Datum
test_resowner_priorities(PG_FUNCTION_ARGS)
{
    int32 nkinds = PG_GETARG_INT32(0);
    int32 nresources = PG_GETARG_INT32(1);
    ResourceOwner parent, child;
    ResourceOwnerDesc *before_desc, *after_desc;

    // Validate parameters
    if (nkinds <= 0 || nresources <= 0)
        elog(ERROR, "nkinds and nresources must be greater than zero");

    // Create parent and child resource owners
    parent = ResourceOwnerCreate(CurrentResourceOwner, "test parent");
    child = ResourceOwnerCreate(parent, "test child");

    // Set up resource descriptors for before-locks phase
    before_desc = palloc(nkinds * sizeof(ResourceOwnerDesc));
    for (int i = 0; i < nkinds; i++) {
        before_desc[i].name = psprintf("test resource before locks %d", i);
        before_desc[i].release_phase = RESOURCE_RELEASE_BEFORE_LOCKS;
        before_desc[i].release_priority = RELEASE_PRIO_FIRST + i;
        before_desc[i].ReleaseResource = ReleaseString;
        before_desc[i].DebugPrint = PrintString;
    }

    // Set up resource descriptors for after-locks phase
    after_desc = palloc(nkinds * sizeof(ResourceOwnerDesc));
    for (int i = 0; i < nkinds; i++) {
        after_desc[i].name = psprintf("test resource after locks %d", i);
        after_desc[i].release_phase = RESOURCE_RELEASE_AFTER_LOCKS;
        after_desc[i].release_priority = RELEASE_PRIO_FIRST + i;
        after_desc[i].ReleaseResource = ReleaseString;
        after_desc[i].DebugPrint = PrintString;
    }

    // Add resources to child and parent with different priorities
    for (int i = 0; i < nresources; i++) {
        ResourceOwnerDesc *kind = &before_desc[i % nkinds];
        ResourceOwnerEnlarge(child);
        ResourceOwnerRemember(child,
                              CStringGetDatum(psprintf("child before locks priority %d", kind->release_priority)),
                              kind);
    }

    for (int i = 0; i < nresources; i++) {
        ResourceOwnerDesc *kind = &after_desc[i % nkinds];
        ResourceOwnerEnlarge(child);
        ResourceOwnerRemember(child,
                              CStringGetDatum(psprintf("child after locks priority %d", kind->release_priority)),
                              kind);
    }

    // Add similar resources to parent
    for (int i = 0; i < nresources; i++) {
        ResourceOwnerDesc *kind = &after_desc[i % nkinds];
        ResourceOwnerEnlarge(parent);
        ResourceOwnerRemember(parent,
                              CStringGetDatum(psprintf("parent after locks priority %d", kind->release_priority)),
                              kind);
    }

    for (int i = 0; i < nresources; i++) {
        ResourceOwnerDesc *kind = &before_desc[i % nkinds];
        ResourceOwnerEnlarge(parent);
        ResourceOwnerRemember(parent,
                              CStringGetDatum(psprintf("parent before locks priority %d", kind->release_priority)),
                              kind);
    }

    // Release resources in proper order
    elog(NOTICE, "releasing resources before locks");
    ResourceOwnerRelease(parent, RESOURCE_RELEASE_BEFORE_LOCKS, false, false);
    elog(NOTICE, "releasing locks");
    ResourceOwnerRelease(parent, RESOURCE_RELEASE_LOCKS, false, false);
    elog(NOTICE, "releasing resources after locks");
    ResourceOwnerRelease(parent, RESOURCE_RELEASE_AFTER_LOCKS, false, false);

    ResourceOwnerDelete(parent);
    PG_RETURN_VOID();
}
```