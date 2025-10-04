# test_resowner_many

## Location
[src/test/modules/test_resowner/test_resowner_many.c:204-296](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_resowner/test_resowner_many.c#L204-L296)

## Overview
A PostgreSQL test function that validates ResourceOwner functionality by managing large numbers of resources across multiple resource types with different release priorities and phases.

## Definition
```c
PG_FUNCTION_INFO_V1(test_resowner_many);
Datum test_resowner_many(PG_FUNCTION_ARGS)
```

## Detailed Description
test_resowner_many is a comprehensive test function for PostgreSQL's ResourceOwner system. It creates and manages multiple resource types with different release phases (before locks and after locks) and priorities to ensure proper resource cleanup ordering. The function takes five integer parameters that control how many resource kinds to create, how many resources to remember in each phase, and how many to forget before cleanup.

The test follows this sequence:
1. Creates multiple resource kinds for both before-locks and after-locks phases
2. Remembers resources in both phases according to the specified counts  
3. Forgets some resources as specified by the parameters
4. Releases resources in the proper order (before locks, locks, after locks)
5. Validates that all resources were properly cleaned up using assertions
6. Cleans up the ResourceOwner

This function is crucial for verifying that PostgreSQL's resource management system correctly handles complex scenarios with multiple resource types and proper cleanup ordering.

## Parameters / Member Variables
- `nkinds`: Number of different resource kinds to create (must be >= 0)
- `nremember_bl`: Number of before-locks resources to remember (must be >= 0)  
- `nforget_bl`: Number of before-locks resources to forget (must be between 0 and nremember_bl)
- `nremember_al`: Number of after-locks resources to remember (must be >= 0)
- `nforget_al`: Number of after-locks resources to forget (must be between 0 and nremember_al)

## Dependencies
- Functions called/Symbols referenced:
  - [ResourceOwner](../R/ResourceOwner.md)
  - [ManyTestResourceKind](../M/ManyTestResourceKind.md)
  - [InitManyTestResourceKind](../I/InitManyTestResourceKind.md)  
  - [RememberManyTestResources](../R/RememberManyTestResources.md)
  - [ForgetManyTestResources](../F/ForgetManyTestResources.md)
  - [GetTotalResourceCount](../G/GetTotalResourceCount.md)
  - [ResourceOwnerCreate](../R/ResourceOwnerCreate.md)
  - [ResourceOwnerRelease](../R/ResourceOwnerRelease.md)
  - [ResourceOwnerDelete](../R/ResourceOwnerDelete.md)
  - PG_GETARG_INT32
  - PG_RETURN_VOID
- Called from (representative examples):
  - [GetTotalResourceCount](../G/GetTotalResourceCount.md) (referenced by this function)

## Notes and Other Information
- This is a PostgreSQL extension function marked with PG_FUNCTION_INFO_V1 macro
- Performs extensive parameter validation and emits ERROR messages for invalid inputs
- Uses NOTICE level logging to report progress through different test phases  
- Creates a dedicated "TestOwner" ResourceOwner for isolation during testing
- Tests both RESOURCE_RELEASE_BEFORE_LOCKS and RESOURCE_RELEASE_AFTER_LOCKS phases
- Uses assertions to validate that all resources are properly cleaned up
- Located in src/test/modules/test_resowner/test_resowner_many.c at lines 203-296
- Part of PostgreSQL's test suite for validating resource owner functionality under complex scenarios

## Simplified Source

```c
// Simplified version of test_resowner_many
Datum
test_resowner_many(PG_FUNCTION_ARGS)
{
    int32 nkinds = PG_GETARG_INT32(0);
    int32 nremember_bl = PG_GETARG_INT32(1);
    int32 nforget_bl = PG_GETARG_INT32(2);
    int32 nremember_al = PG_GETARG_INT32(3);
    int32 nforget_al = PG_GETARG_INT32(4);

    ResourceOwner resowner;
    ManyTestResourceKind *before_kinds, *after_kinds;

    // Validate parameters
    if (nkinds < 0 || nremember_bl < 0 || nremember_al < 0)
        elog(ERROR, "negative values not allowed");
    if (nforget_bl < 0 || nforget_bl > nremember_bl)
        elog(ERROR, "nforget_bl must be between 0 and nremember_bl");
    if (nforget_al < 0 || nforget_al > nremember_al)
        elog(ERROR, "nforget_al must be between 0 and nremember_al");

    // Initialize resource kinds for both phases
    before_kinds = palloc(nkinds * sizeof(ManyTestResourceKind));
    for (int i = 0; i < nkinds; i++) {
        InitManyTestResourceKind(&before_kinds[i],
                                 psprintf("resource before locks %d", i),
                                 RESOURCE_RELEASE_BEFORE_LOCKS,
                                 RELEASE_PRIO_FIRST + i);
    }

    after_kinds = palloc(nkinds * sizeof(ManyTestResourceKind));
    for (int i = 0; i < nkinds; i++) {
        InitManyTestResourceKind(&after_kinds[i],
                                 psprintf("resource after locks %d", i),
                                 RESOURCE_RELEASE_AFTER_LOCKS,
                                 RELEASE_PRIO_FIRST + i);
    }

    // Create test resource owner
    resowner = ResourceOwnerCreate(CurrentResourceOwner, "TestOwner");

    // Remember resources
    if (nremember_bl > 0) {
        elog(NOTICE, "remembering %d before-locks resources", nremember_bl);
        RememberManyTestResources(resowner, before_kinds, nkinds, nremember_bl);
    }
    if (nremember_al > 0) {
        elog(NOTICE, "remembering %d after-locks resources", nremember_al);
        RememberManyTestResources(resowner, after_kinds, nkinds, nremember_al);
    }

    // Forget some resources
    if (nforget_bl > 0) {
        elog(NOTICE, "forgetting %d before-locks resources", nforget_bl);
        ForgetManyTestResources(resowner, before_kinds, nkinds, nforget_bl);
    }
    if (nforget_al > 0) {
        elog(NOTICE, "forgetting %d after-locks resources", nforget_al);
        ForgetManyTestResources(resowner, after_kinds, nkinds, nforget_al);
    }

    // Release resources in proper order
    elog(NOTICE, "releasing resources before locks");
    current_release_phase = RESOURCE_RELEASE_BEFORE_LOCKS;
    last_release_priority = 0;
    ResourceOwnerRelease(resowner, RESOURCE_RELEASE_BEFORE_LOCKS, false, false);

    elog(NOTICE, "releasing locks");
    current_release_phase = RESOURCE_RELEASE_LOCKS;
    last_release_priority = 0;
    ResourceOwnerRelease(resowner, RESOURCE_RELEASE_LOCKS, false, false);

    elog(NOTICE, "releasing resources after locks");
    current_release_phase = RESOURCE_RELEASE_AFTER_LOCKS;
    last_release_priority = 0;
    ResourceOwnerRelease(resowner, RESOURCE_RELEASE_AFTER_LOCKS, false, false);

    // Verify all resources were cleaned up
    Assert(GetTotalResourceCount(before_kinds, nkinds) == 0);
    Assert(GetTotalResourceCount(after_kinds, nkinds) == 0);

    ResourceOwnerDelete(resowner);
    PG_RETURN_VOID();
}
```