# test_resowner_remember_between_phases

## Location
[src/test/modules/test_resowner/test_resowner_basic.c:165-187](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_resowner/test_resowner_basic.c#L165-L187)

## Overview
test_resowner_remember_between_phases is a PostgreSQL SQL-callable function that tests the resource owner's validation of resource registration attempts during the resource release process.

## Definition
```c
Datum test_resowner_remember_between_phases(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a negative test case for PostgreSQL's resource owner system, specifically testing the enforcement of proper resource management phase ordering. It creates a resource owner, initiates the resource release process by calling ResourceOwnerRelease for the BEFORE_LOCKS phase, and then attempts to register a new resource. This scenario should fail because the resource owner is already in the release phase and should not accept new resource registrations.

The function is designed to trigger an error condition when ResourceOwnerEnlarge is called after ResourceOwnerRelease has already been initiated, demonstrating that the resource owner correctly validates the state transitions and prevents improper resource registration during cleanup phases.

## Parameters / Member Variables
This function takes no parameters (uses PG_FUNCTION_ARGS macro but doesn't extract any arguments).

## Dependencies
- Functions called/Symbols referenced:
  - [ResourceOwnerCreate](../R/ResourceOwnerCreate.md) (resource owner creation)
  - [ResourceOwnerRelease](../R/ResourceOwnerRelease.md) (initiates resource release process)
  - [ResourceOwnerEnlarge](../R/ResourceOwnerEnlarge.md) (attempts resource owner capacity expansion - should fail)
  - [ResourceOwnerRemember](../R/ResourceOwnerRemember.md) (attempts resource registration - should not be reached)
  - [CStringGetDatum](../C/CStringGetDatum.md) (string to Datum conversion)
  - string_desc (global resource descriptor for string resources)
  - elog (error logging - for unreachable code path)
  - PG_RETURN_VOID (return macro)
- Called from (representative examples):
  - SQL queries invoking this test function

## Notes and Other Information
- Part of the test_resowner module for testing PostgreSQL resource management
- Designed as a negative test case that should trigger an error condition
- Tests the resource owner's state validation during phase transitions
- The elog(ERROR, "ResourceOwnerEnlarge should have errored out") line should never be reached if the system works correctly
- References string_desc which should be defined elsewhere in the module as a ResourceOwnerDesc
- Demonstrates PostgreSQL's defensive programming approach by validating operation sequences
- The function should terminate with an error before reaching the PG_RETURN_VOID() statement
- Useful for verifying that resource owners properly enforce phase ordering and prevent invalid state transitions

## Simplified Source

```c
// Simplified version of test_resowner_remember_between_phases
Datum
test_resowner_remember_between_phases(PG_FUNCTION_ARGS)
{
    ResourceOwner resowner;

    // Create test resource owner
    resowner = ResourceOwnerCreate(CurrentResourceOwner, "TestOwner");

    // Start resource release process
    ResourceOwnerRelease(resowner, RESOURCE_RELEASE_BEFORE_LOCKS, true, false);

    // Try to remember a new resource after release has started
    // This should fail with an error
    ResourceOwnerEnlarge(resowner);
    ResourceOwnerRemember(resowner, CStringGetDatum("my string"), &string_desc);

    // This line should never be reached
    elog(ERROR, "ResourceOwnerEnlarge should have errored out");
    PG_RETURN_VOID();
}
```