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
- `owner`: ResourceOwner instance from which to remove resources
- `*kinds`: Array of ManyTestResourceKind structures defining resource types
- `nkinds`: Number of resource kinds in the kinds array
- `nresources`: Number of resources to forget and deallocate
## Dependencies
- Functions called/Symbols referenced:
  - [GetTotalResourceCount](../G/GetTotalResourceCount.md) (helper function to count available resources)
  - elog (logging function for PANIC and ERROR)
  - [dlist_is_empty](../d/dlist_is_empty.md) (check if resource list is empty)
  - dlist_head_element (get first element from list)
  - [ResourceOwnerForget](../R/ResourceOwnerForget.md) (unregister resource from owner)
  - [dlist_delete](../d/dlist_delete.md) (remove from doubly-linked list)
  - [pfree](../p/pfree.md) (PostgreSQL memory deallocation)
  - [PointerGetDatum](../P/PointerGetDatum.md) (convert pointer to Datum)
- Called from (representative examples):
  - [test_resowner_many](../t/test_resowner_many.md) (main test function)

## Notes and Other Information
- This is a static function, only accessible within the test_resowner_many.c file
- The function includes safety checks to prevent attempts to forget more resources than exist
- Uses a PANIC level error if insufficient resources are available, indicating a serious testing issue
- Implements a round-robin search with fallback to handle uneven resource distribution
- Updates the nforgotten statistics counter for each successfully removed resource
- Memory deallocation follows PostgreSQL conventions using pfree()
- Will generate an ERROR if it cannot find any resource to forget during the search loop

## Simplified Source

```c
// Simplified version of ForgetManyTestResources
static void
ForgetManyTestResources(ResourceOwner owner,
                        ManyTestResourceKind *kinds, int nkinds,
                        int nresources)
{
    int kind_idx = 0;
    int ntotal = GetTotalResourceCount(kinds, nkinds);

    // Validate sufficient resources exist
    if (ntotal < nresources)
        elog(PANIC, "cannot free %d resources, only %d remembered", nresources, ntotal);

    // Forget resources in round-robin fashion
    for (int i = 0; i < nresources; i++) {
        bool found = false;

        // Search across all kinds for available resources
        for (int j = 0; j < nkinds; j++) {
            kind_idx = (kind_idx + 1) % nkinds;
            if (!dlist_is_empty(&kinds[kind_idx].current_resources)) {
                // Get first resource from this kind
                ManyTestResource *mres = dlist_head_element(ManyTestResource, node,
                                                            &kinds[kind_idx].current_resources);

                // Unregister and free resource
                ResourceOwnerForget(owner, PointerGetDatum(mres), &kinds[kind_idx].desc);
                kinds[kind_idx].nforgotten++;
                dlist_delete(&mres->node);
                pfree(mres);

                found = true;
                break;
            }
        }

        if (!found)
            elog(ERROR, "could not find a test resource to forget");
    }
}
```