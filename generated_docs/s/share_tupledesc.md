# share_tupledesc

## Location
[src/backend/utils/cache/typcache.c:2735-2755](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/typcache.c#L2735-L2755)

## Overview
Copies a tuple descriptor into shared memory with a specified typmod value and returns a pointer to the shared copy.

## Definition

```c
static dsa_pointer
share_tupledesc(dsa_area *area, TupleDesc tupdesc, uint32 typmod)
```
## Detailed Description
The  function creates a copy of a tuple descriptor in shared memory managed by a dynamic shared area (DSA). It allocates space in the shared memory area sufficient to hold the tuple descriptor, copies the original descriptor using , and sets the typmod field to the specified value. The function returns a  that can be used to access the shared tuple descriptor from any process with access to the same DSA area.

## Parameters / Member Variables
- `*area`: Pointer to the dynamic shared area where the tuple descriptor will be allocated
- `tupdesc`: The source tuple descriptor to copy into shared memory
- `typmod`: The type modifier value to set in the shared copy
## Dependencies
- Functions called/Symbols referenced:
  - dsa_allocate (allocates memory in the dynamic shared area)
  - [dsa_get_address](../d/dsa_get_address.md) (converts dsa_pointer to local address)
  - TupleDescSize (calculates the size needed for the tuple descriptor)
  - [TupleDescCopy](../T/TupleDescCopy.md) (copies tuple descriptor data)
- Data structures used:
  - [dsa_area](../d/dsa_area.md)
  - dsa_pointer
  - [TupleDesc](../T/TupleDesc.md)
- Called from (representative examples):
  - [SharedRecordTypmodRegistryInit](../S/SharedRecordTypmodRegistryInit.md) (during registry initialization)
  - [find_or_make_matching_shared_tupledesc](../f/find_or_make_matching_shared_tupledesc.md) (when creating new shared descriptors)

## Notes and Other Information
- The function is static and only used within the typcache.c module
- The shared tuple descriptor can be accessed by multiple processes in a PostgreSQL cluster
- Used primarily for sharing record type information across parallel workers
- The typmod field is explicitly set after copying, allowing customization of the type modifier
- Memory allocation in shared area enables efficient sharing without serialization overhead

## Simplified Source
```c
static dsa_pointer
share_tupledesc(dsa_area *area, TupleDesc tupdesc, uint32 typmod)
{
    dsa_pointer shared_dp;
    TupleDesc shared;

    // Allocate space in shared memory
    shared_dp = dsa_allocate(area, TupleDescSize(tupdesc));

    // Get local address for the shared memory
    shared = (TupleDesc) dsa_get_address(area, shared_dp);

    // Copy tuple descriptor and set typmod
    TupleDescCopy(shared, tupdesc);
    shared->tdtypmod = typmod;

    return shared_dp;
}
```