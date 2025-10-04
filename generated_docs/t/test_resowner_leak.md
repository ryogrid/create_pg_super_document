# test_resowner_leak

## Location
[src/test/modules/test_resowner/test_resowner_basic.c:142-164](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_resowner/test_resowner_basic.c#L142-L164)

## Overview
test_resowner_leak is a PostgreSQL SQL-callable function that deliberately creates a resource leak scenario to test the resource owner's handling of unreleased resources during cleanup.

## Definition
```c
Datum test_resowner_leak(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a test case for PostgreSQL's resource owner leak detection and cleanup mechanisms. It intentionally creates a resource owner, registers a string resource with it, and then performs a complete resource release cycle without explicitly forgetting the resource first. This simulates a resource leak scenario where a resource is not properly cleaned up by application code, allowing the resource owner subsystem to demonstrate its automatic cleanup capabilities.

The function creates a resource owner, adds a test string resource, and then calls the three-phase resource release process (before locks, locks, after locks) with the isTopLevel flag set to true. The resource is intentionally not removed via ResourceOwnerForget, creating a controlled leak scenario for testing purposes.

## Parameters / Member Variables
This function takes no parameters (uses PG_FUNCTION_ARGS macro but doesn't extract any arguments).

## Dependencies
- Functions called/Symbols referenced:
  - [ResourceOwnerCreate](../R/ResourceOwnerCreate.md) (resource owner creation)
  - [ResourceOwnerEnlarge](../R/ResourceOwnerEnlarge.md) (resource owner capacity management)  
  - [ResourceOwnerRemember](../R/ResourceOwnerRemember.md) (resource registration)
  - [ResourceOwnerRelease](../R/ResourceOwnerRelease.md) (resource cleanup in three phases)
  - [ResourceOwnerDelete](../R/ResourceOwnerDelete.md) (resource owner cleanup)
  - [CStringGetDatum](../C/CStringGetDatum.md) (string to Datum conversion)
  - string_desc (global resource descriptor for string resources)
  - PG_RETURN_VOID (return macro)
- Called from (representative examples):
  - SQL queries invoking this test function

## Notes and Other Information
- Part of the test_resowner module for testing PostgreSQL resource management
- Deliberately creates a resource leak to test automatic cleanup mechanisms
- Uses isTopLevel=true parameter in ResourceOwnerRelease calls to enable top-level cleanup behavior
- The leaked resource should be automatically cleaned up by the resource owner during the release phases
- References string_desc, which should be defined elsewhere in the module as a ResourceOwnerDesc
- Demonstrates the robustness of PostgreSQL's resource management system in handling improperly cleaned resources
- Returns void as it is primarily a testing function
- Useful for verifying that resource owners properly handle leaked resources during transaction cleanup

## Simplified Source

```c
// Simplified version of test_resowner_leak
Datum
test_resowner_leak(PG_FUNCTION_ARGS)
{
    ResourceOwner resowner;

    // Create test resource owner
    resowner = ResourceOwnerCreate(CurrentResourceOwner, "TestOwner");

    // Remember a resource but deliberately don't forget it (create leak)
    ResourceOwnerEnlarge(resowner);
    ResourceOwnerRemember(resowner, CStringGetDatum("my string"), &string_desc);

    // Release resources in all phases - should handle the leak automatically
    ResourceOwnerRelease(resowner, RESOURCE_RELEASE_BEFORE_LOCKS, true, false);
    ResourceOwnerRelease(resowner, RESOURCE_RELEASE_LOCKS, true, false);
    ResourceOwnerRelease(resowner, RESOURCE_RELEASE_AFTER_LOCKS, true, false);

    ResourceOwnerDelete(resowner);
    PG_RETURN_VOID();
}
```