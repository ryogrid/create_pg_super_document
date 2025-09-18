# inet_gist_union

## Location
src/backend/utils/adt/network_gist.c: 505 - 541

## Overview
The GiST union function for inet data types that computes the union of multiple GiST index entries to create a single key representing the combined range.

## Definition


## Detailed Description
This function implements the GiST union method for inet/cidr data types, which is called during index construction and maintenance to combine multiple child keys into a single parent key. The union operation creates a key that represents the tightest possible bounding box containing all input inet values.

The function follows a three-step process:
1. **Parameter calculation**: Uses calc_inet_union_params() to analyze all input keys and determine union characteristics (family range, minimum bits, common bits)
2. **Family handling**: If multiple address families are present, sets the family to 0 (indicating mixed families)
3. **Key construction**: Uses build_inet_union_key() to create the final union key with calculated parameters

The resulting union key maintains GiST's requirement that all child values must be contained within (or equal to) the parent key's representation. For inet types, this means the union key's network range encompasses all input network ranges.

## Parameters / Member Variables
- : GistEntryVector containing all the keys to be combined into a union

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POINTER, PG_RETURN_POINTER
  - [GistEntryVector](../G/GistEntryVector.md), GISTENTRY
  - [calc_inet_union_params](../c/calc_inet_union_params.md)
  - DatumGetInetKeyP
  - gk_ip_addr
  - [build_inet_union_key](../b/build_inet_union_key.md)
- Called from (representative examples):
  - GiST index access methods (indirectly through function pointer)

## Notes and Other Information
- This function is registered as the union method in the inet GiST opclass
- Essential for maintaining GiST index structure during splits and insertions
- The union operation is associative and commutative, allowing flexible combination order
- Mixed address families result in a family-neutral union key (family = 0)
- The function ensures the containment property required by GiST: union(A,B) contains both A and B
- Used during internal index node construction and page splitting operations