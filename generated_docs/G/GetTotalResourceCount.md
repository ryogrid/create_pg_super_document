# GetTotalResourceCount

## Location
[src/test/modules/test_resowner/test_resowner_many.c:185-203](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_resowner/test_resowner_many.c#L185-L203)

## Overview
Calculates and returns the total number of currently active resources across multiple resource types in the test_resowner_many test module.

## Definition
```c
static int GetTotalResourceCount(ManyTestResourceKind *kinds, int nkinds)
```

## Detailed Description
GetTotalResourceCount is a utility function used in PostgreSQL's resource owner testing framework. It iterates through an array of ManyTestResourceKind structures and calculates the total number of active resources by summing up the difference between remembered, forgotten, and released resources for each kind. This function helps verify that resource tracking is working correctly during testing scenarios where multiple resource types with different priorities are managed simultaneously.

The function performs the calculation: `nremembered - nforgotten - nreleased` for each resource kind, which gives the net number of active resources for that particular type.

## Parameters / Member Variables
- `kinds`: Array of ManyTestResourceKind structures representing different resource types being tracked
- `nkinds`: Number of elements in the kinds array

## Dependencies
- Functions called/Symbols referenced:
  - [ManyTestResourceKind](../M/ManyTestResourceKind.md) (struct type)
- Called from (representative examples):
  - [test_resowner_many](../t/test_resowner_many.md) (at lines 279, 290, 291)

## Notes and Other Information
- This is a static function, only accessible within the test_resowner_many.c file
- Used specifically for testing PostgreSQL's ResourceOwner functionality
- The function accesses the nremembered, nforgotten, and nreleased counters maintained by the ManyTestResourceKind structure to track resource lifecycle events
- Located in src/test/modules/test_resowner/test_resowner_many.c at lines 181-193