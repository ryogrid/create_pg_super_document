# InitManyTestResourceKind

## Location
src/test/modules/test_resowner/test_resowner_many.c: 97 - 116

## Overview
Initializes a ManyTestResourceKind structure by setting up its resource descriptor, callback functions, and internal state for resource management testing.

## Definition


## Detailed Description
InitManyTestResourceKind is a constructor function that properly initializes a ManyTestResourceKind structure for use in PostgreSQL's resource management testing framework. This function sets up all the necessary components that define how a particular kind of test resource should be managed.

The function performs comprehensive initialization:
1. Configures the resource descriptor with the provided name, release phase, and priority
2. Assigns the appropriate callback functions for resource cleanup and debug printing
3. Initializes all statistical counters to zero
4. Sets up the doubly-linked list for tracking current resources

The resource descriptor is populated with callback functions that will be invoked by PostgreSQL's ResourceOwner system during resource cleanup (ReleaseManyTestResource) and leak detection (PrintManyTest). The statistical counters track various resource operations throughout the test lifecycle, providing valuable debugging and validation information.

## Parameters / Member Variables
- : Pointer to the ManyTestResourceKind structure to be initialized
- : Descriptive name for this resource kind (used in logging and debugging)
- : ResourceReleasePhase indicating when this resource should be released during cleanup
- : Priority value determining the order of resource release within the same phase

## Dependencies
- Functions called/Symbols referenced:
  - ReleaseManyTestResource (assigned as callback)
  - PrintManyTest (assigned as debug print callback)
  - dlist_init (initializes doubly-linked list)
- Called from (representative examples):
  - test_resowner_many (main test function)

## Notes and Other Information
- This is a static function, only accessible within the test_resowner_many.c file
- The function assumes the caller has allocated the ManyTestResourceKind structure
- All statistical counters (nremembered, nforgotten, nreleased, nleaked) are initialized to zero
- The current_resources list is properly initialized as an empty doubly-linked list
- The initialized structure becomes ready for use with RememberManyTestResources and ForgetManyTestResources
- Resource release phase and priority determine cleanup ordering in PostgreSQL's resource management system