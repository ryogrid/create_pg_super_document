# test_resowner_priorities

## Location
src/test/modules/test_resowner/test_resowner_basic.c: 52 - 141

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
  - ResourceOwnerCreate (resource owner creation)
  - ResourceOwnerEnlarge (resource owner capacity management)
  - ResourceOwnerRemember (resource registration)
  - ResourceOwnerRelease (resource cleanup)
  - ResourceOwnerDelete (resource owner cleanup)
  - CStringGetDatum (string to Datum conversion)
  - ReleaseString (custom resource release callback)
  - PrintString (custom resource print callback)
  - elog (logging)
  - palloc (memory allocation)
  - psprintf (formatted string creation)
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