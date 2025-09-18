# build_attrmap_by_name_if_req

## Location
src/backend/access/common/attmap.c: 263 - 289

## Overview
A convenience wrapper function that builds an attribute map by name but returns NULL if no conversion is required, optimizing for cases where runtime conversion might be unnecessary.

## Definition
```c
AttrMap *build_attrmap_by_name_if_req(TupleDesc indesc, TupleDesc outdesc, bool missing_ok)
```

## Detailed Description
The `build_attrmap_by_name_if_req` function is a convenience wrapper around `build_attrmap_by_name` that adds an optimization step. After building the attribute map, it checks whether the mapping represents a perfect one-to-one correspondence between input and output descriptors. If so, it frees the map and returns NULL to indicate that no runtime conversion is needed, saving memory and processing time.

This function is particularly useful in scenarios where tuple conversion might not be necessary (e.g., when working with identical schemas), as it automatically detects such cases and avoids unnecessary overhead. The function maintains the same name-based matching semantics as `build_attrmap_by_name` but provides better performance for the common case of schema identity.

## Parameters / Member Variables
- `indesc`: Input tuple descriptor containing source columns
- `outdesc`: Output tuple descriptor containing target columns
- `missing_ok`: If true, missing columns in input are tolerated; if false, missing columns cause an error

## Dependencies
- Functions called/Symbols referenced:
  - `build_attrmap_by_name` (creates the name-based attribute map)
  - `check_attrmap_match` (verifies if one-to-one mapping exists)
  - `free_attrmap` (deallocates map when not needed)
- Called from (representative examples):
  - `convert_tuples_by_name`
  - `addFkRecurseReferenced`
  - `ExecPartitionCheckEmitError`
  - `ExecConstraints`
  - `ExecWithCheckOptions`
  - `ExecInitPartitionDispatchInfo`
  - `init_tuple_slot`

## Notes and Other Information
- Returns NULL when no runtime conversion is required (perfect schema match)
- Provides automatic optimization by detecting unnecessary conversions
- Maintains all the name-based matching capabilities of `build_attrmap_by_name`
- Commonly used in tuple conversion scenarios where schema compatibility is uncertain
- Especially useful in partitioning, constraint checking, and replication contexts
- The "if_req" suffix indicates the conditional nature - map only returned if required
- Located in `src/backend/access/common/attmap.c:263-289`