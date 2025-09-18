# bms_nonempty_difference

## Location
src/backend/nodes/bitmapset.c: 641 - 671

## Overview
Tests whether two bitmapsets have a nonempty difference, i.e., whether the first bitmapset contains any members that are not present in the second bitmapset.

## Definition
```c
bool bms_nonempty_difference(const Bitmapset *a, const Bitmapset *b)
```

## Detailed Description
This function determines whether bitmapset 'a' contains any members that are not also present in bitmapset 'b'. It performs an efficient bitwise comparison to detect if there are any bits set in 'a' that are not set in 'b'. The function uses several optimizations: it immediately returns false if 'a' is NULL, returns true if 'a' has content but 'b' is NULL, and returns true if 'a' has more words than 'b' (indicating additional members). For the overlapping portion, it uses bitwise AND with complement operations to efficiently detect differences.

## Parameters / Member Variables
- `a`: The first bitmapset to check for unique members (const Bitmapset *)
- `b`: The second bitmapset to compare against (const Bitmapset *)

## Dependencies
- Functions called/Symbols referenced:
  - bms_is_valid_set
- Called from (representative examples):
  - ExecReScanMemoize (src/backend/executor/nodeMemoize.c:1162)
  - bms_difference (src/backend/nodes/bitmapset.c:365)
  - allow_star_schema_join (src/backend/optimizer/path/joinpath.c:372)
  - use_physical_tlist (src/backend/optimizer/plan/createplan.c:935)
  - add_placeholders_to_base_rels (src/backend/optimizer/util/placeholder.c:340)
  - build_joinrel_tlist (src/backend/optimizer/util/relnode.c:1136, 1205)

## Notes and Other Information
- Returns false if the first bitmapset is NULL (empty set has no unique members)
- Returns true if the first bitmapset is non-NULL but the second is NULL
- Uses efficient early termination when 'a' has more words than 'b'
- Employs bitwise operations (a->words[i] & ~b->words[i]) for efficient difference detection
- Commonly used in query optimization to determine set relationships
- Located in src/backend/nodes/bitmapset.c:641-671