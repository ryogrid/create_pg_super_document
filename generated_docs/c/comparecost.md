# comparecost

## Location
[src/backend/utils/adt/tsquery_gist.c:158-163](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsquery_gist.c#L158-L163)

## Overview
A comparison function used for sorting SPLITCOST structures by their cost values during GiST index node splitting operations in text search indexing.

## Definition


## Detailed Description
The comparecost function is a qsort-compatible comparison function that compares two SPLITCOST structures based on their cost values. This function is essential for the GiST (Generalized Search Tree) index splitting algorithm, where different split options are evaluated and sorted by cost to determine the optimal way to split an index node.

The function takes two void pointers (as required by qsort), casts them to SPLITCOST pointers, and compares their cost fields using PostgreSQL's pg_cmp_s32 utility function. This enables efficient sorting of split candidates during index construction and maintenance.

## Parameters / Member Variables
- `va`: Pointer to the first SPLITCOST structure to compare
- `vb`: Pointer to the second SPLITCOST structure to compare

## Dependencies
- Functions called/Symbols referenced:
  - SPLITCOST (structure type containing split cost information)
  - [pg_cmp_s32](../p/pg_cmp_s32.md) (PostgreSQL utility function for comparing 32-bit signed integers)
- Called from:
  - [gtsvector_picksplit](../g/gtsvector_picksplit.md) (in tsgistidx.c)
  - [gtsquery_picksplit](../g/gtsquery_picksplit.md) (in tsquery_gist.c)

## Notes and Other Information
- This function follows the standard qsort comparison function interface
- Returns negative, zero, or positive value for less than, equal to, or greater than relationships
- Static function only accessible within the tsgistidx.c compilation unit
- Critical for optimal GiST index performance by ensuring splits minimize storage cost
- Used in conjunction with split cost calculation algorithms
- Located in src/backend/utils/adt/tsgistidx.c:595-604