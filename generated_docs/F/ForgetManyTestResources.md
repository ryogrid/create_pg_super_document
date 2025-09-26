# ForgetManyTestResources

## Location
[src/test/modules/test_resowner/test_resowner_many.c:145-184](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_resowner/test_resowner_many.c#L145-L184)

## Overview
Unregisters and deallocates multiple test resources from the ResourceOwner system, removing them in round-robin fashion across different resource kinds.

## Definition

```c
static void
ForgetManyTestResources(ResourceOwner owner,
						ManyTestResourceKind *kinds, int nkinds,
						int nresources)
```
## Detailed Description
ForgetManyTestResources is a utility function that removes and deallocates multiple ManyTestResource objects from PostgreSQL's ResourceOwner system during testing. The function operates as the counterpart to RememberManyTestResources, systematically unregistering resources while maintaining proper cleanup and statistics.

The function first validates that sufficient resources exist by calling GetTotalResourceCount and comparing against the requested number of resources to forget. If insufficient resources are available, it triggers a PANIC to prevent data corruption.

For each resource to be forgotten, the function uses a round-robin search strategy across all resource kinds to find available resources. When a resource is found:
1. It unregisters the resource from the ResourceOwner system
2. Updates the forgotten count statistics for that resource kind
3. Removes the resource from the tracking list
4. Deallocates the memory

The round-robin approach with fallback searching ensures even distribution of resource removal across kinds while handling cases where some kinds may be exhausted before others.

## Parameters / Member Variables
- : ResourceOwner instance from which to remove resources
- : Array of ManyTestResourceKind structures defining resource types
- : Number of resource kinds in the kinds array
- : Number of resources to forget and deallocate

## Dependencies
- Functions called/Symbols referenced:
  - GetTotalResourceCount (helper function to count available resources)
  - elog (logging function for PANIC and ERROR)
  - dlist_is_empty (check if resource list is empty)
  - dlist_head_element (get first element from list)
  - ResourceOwnerForget (unregister resource from owner)
  - dlist_delete (remove from doubly-linked list)
  - pfree (PostgreSQL memory deallocation)
  - PointerGetDatum (convert pointer to Datum)
- Called from (representative examples):
  - test_resowner_many (main test function)

## Notes and Other Information
- This is a static function, only accessible within the test_resowner_many.c file
- The function includes safety checks to prevent attempts to forget more resources than exist
- Uses a PANIC level error if insufficient resources are available, indicating a serious testing issue
- Implements a round-robin search with fallback to handle uneven resource distribution
- Updates the nforgotten statistics counter for each successfully removed resource
- Memory deallocation follows PostgreSQL conventions using pfree()
- Will generate an ERROR if it cannot find any resource to forget during the search loop