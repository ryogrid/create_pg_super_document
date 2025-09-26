# ManyTestResourceKind

## Location
src/test/modules/test_resowner/test_resowner_many.c: 38 - 43

## Overview
ManyTestResourceKind is a custom resource type definition used in PostgreSQL's resource owner testing framework to track multiple test resources and verify that ResourceOwner callback functions work correctly.

## Definition

```c
typedef struct
{
	ManyTestResourceKind *kind;
	dlist_node	node;
} ManyTestResource;
```
## Detailed Description
ManyTestResourceKind is a specialized structure designed for testing PostgreSQL's ResourceOwner functionality with large numbers of resources. It extends the basic ResourceOwnerDesc with additional tracking counters and a linked list to maintain cross-references of remembered resources. This allows the test framework to verify that the ResourceOwner system correctly calls callback functions for resource management operations like remember, forget, release, and leak detection.

The structure serves as both a resource type descriptor and a tracking mechanism to ensure that resource management operations are performed in the correct order and with proper counts. It's specifically used in the test_resowner_many.c module to validate resource owner behavior under high-volume scenarios.

## Parameters / Member Variables
- : ResourceOwnerDesc structure containing callback functions and metadata for this resource type
- : Counter tracking how many times resources of this type have been remembered by the ResourceOwner
- : Counter tracking how many times resources of this type have been forgotten by the ResourceOwner  
- : Counter tracking how many times resources of this type have been released by the ResourceOwner
- : Counter tracking how many times resources of this type have been detected as leaked
- : Doubly-linked list head maintaining the list of currently tracked ManyTestResource instances

## Dependencies
- Functions called/Symbols referenced:
  - ResourceOwnerDesc (embedded structure)
  - dlist_head (for resource tracking list)
  - dlist_node (used by associated ManyTestResource structure)

- Called from (representative examples):
  - InitManyTestResourceKind (initialization function)
  - RememberManyTestResources (resource registration)
  - ForgetManyTestResources (resource deregistration)  
  - GetTotalResourceCount (statistics gathering)
  - test_resowner_many (main test function)

## Notes and Other Information
This structure is part of PostgreSQL's testing infrastructure and is not used in production code. It demonstrates how to create custom resource types that integrate with PostgreSQL's ResourceOwner system. The counters provide verification that callback functions are invoked the correct number of times, while the linked list allows cross-checking that the right resources are being tracked. The structure is initialized using InitManyTestResourceKind() which sets up the ResourceOwnerDesc callbacks and initializes all counters to zero.