# share_tupledesc

## Location
src/backend/utils/cache/typcache.c: 2735 - 2755

## Overview
Copies a tuple descriptor into shared memory with a specified typmod value and returns a pointer to the shared copy.

## Definition


## Detailed Description
The  function creates a copy of a tuple descriptor in shared memory managed by a dynamic shared area (DSA). It allocates space in the shared memory area sufficient to hold the tuple descriptor, copies the original descriptor using , and sets the typmod field to the specified value. The function returns a  that can be used to access the shared tuple descriptor from any process with access to the same DSA area.

## Parameters / Member Variables
- : Pointer to the dynamic shared area where the tuple descriptor will be allocated
- : The source tuple descriptor to copy into shared memory
- : The type modifier value to set in the shared copy

## Dependencies
- Functions called/Symbols referenced:
  - dsa_allocate (allocates memory in the dynamic shared area)
  - dsa_get_address (converts dsa_pointer to local address)
  - TupleDescSize (calculates the size needed for the tuple descriptor)
  - TupleDescCopy (copies tuple descriptor data)
- Data structures used:
  - dsa_area
  - dsa_pointer
  - TupleDesc
- Called from (representative examples):
  - SharedRecordTypmodRegistryInit (during registry initialization)
  - find_or_make_matching_shared_tupledesc (when creating new shared descriptors)

## Notes and Other Information
- The function is static and only used within the typcache.c module
- The shared tuple descriptor can be accessed by multiple processes in a PostgreSQL cluster
- Used primarily for sharing record type information across parallel workers
- The typmod field is explicitly set after copying, allowing customization of the type modifier
- Memory allocation in shared area enables efficient sharing without serialization overhead