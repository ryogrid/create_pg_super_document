# RememberManyTestResources

## Location
[src/test/modules/test_resowner/test_resowner_many.c:117-144](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_resowner/test_resowner_many.c#L117-L144)

## Overview
Allocates and registers multiple test resources with the ResourceOwner system, distributing them across different resource kinds in round-robin fashion.

## Definition

```c
static void
RememberManyTestResources(ResourceOwner owner,
						  ManyTestResourceKind *kinds, int nkinds,
						  int nresources)
```
## Detailed Description
RememberManyTestResources is a utility function that creates and registers multiple ManyTestResource objects with PostgreSQL's ResourceOwner system for testing purposes. The function allocates the specified number of resources and distributes them evenly across the provided resource kinds using a round-robin allocation strategy.

For each resource created, the function performs several operations:
1. Allocates memory for a new ManyTestResource structure
2. Associates it with the current resource kind in the round-robin cycle
3. Initializes the doubly-linked list node for tracking
4. Ensures the ResourceOwner has sufficient capacity
5. Registers the resource with the ResourceOwner system
6. Updates statistics and tracking structures
7. Logs debug information about the resource creation

The round-robin distribution ensures that resources are evenly distributed across all provided resource kinds, which is useful for testing scenarios where you want to verify resource management behavior across multiple resource types with different characteristics.

## Parameters / Member Variables
- `owner`: ResourceOwner instance that will manage these resources
- `*kinds`: Array of ManyTestResourceKind structures defining resource types
- `nkinds`: Number of resource kinds in the kinds array
- `nresources`: Total number of resources to create and remember
## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation)
  - [dlist_node_init](../d/dlist_node_init.md) (initialize doubly-linked list node)
  - [ResourceOwnerEnlarge](ResourceOwnerEnlarge.md) (ensure ResourceOwner capacity)
  - [ResourceOwnerRemember](ResourceOwnerRemember.md) (register resource with owner)
  - [dlist_push_tail](../d/dlist_push_tail.md) (add to end of resource list)
  - elog (logging function)
  - [PointerGetDatum](../P/PointerGetDatum.md) (convert pointer to Datum)
- Called from (representative examples):
  - [test_resowner_many](../t/test_resowner_many.md) (main test function)

## Notes and Other Information
- This is a static function, only accessible within the test_resowner_many.c file
- Resources are distributed in round-robin fashion across all provided resource kinds
- Each created resource is tracked in both the ResourceOwner and the kind's current_resources list
- The function increments the nremembered counter for each resource kind
- Debug logging is performed at DEBUG1 level for each resource creation
- Memory is allocated using PostgreSQL's palloc, making resources subject to memory context cleanup
- The function assumes all parameters are valid and properly initialized