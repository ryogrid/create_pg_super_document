# range_gist_picksplit

## Location
[src/backend/utils/adt/rangetypes_gist.c:619-777](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes_gist.c#L619-L777)

## Overview
The GiST PickSplit method for range types that implements node splitting logic in GiST indexes for range data types by segregating ranges of different classes and applying appropriate split methods.

## Definition

```c
Datum
range_gist_picksplit(PG_FUNCTION_ARGS)
```
## Detailed Description
This function is the core splitting algorithm for GiST (Generalized Search Tree) indexes on range types. It receives a vector of range entries that need to be split into two groups and implements a sophisticated strategy:

1. **Class-based segregation**: First tries to separate ranges of different classes (normal ranges, ranges with infinite bounds, empty ranges, etc.)
2. **Within-class splitting**: If all ranges belong to the same class, applies the most appropriate splitting method for that specific class:
   - Normal ranges: uses double sorting split
   - Lower infinite ranges: uses upper bound sorting split  
   - Upper infinite ranges: uses lower bound sorting split
   - All infinite or empty ranges: uses fallback split
3. **Balanced distribution**: When multiple classes exist, attempts to balance the split by separating ranges with infinities from those without, or ranges containing empty from non-empty ranges

The algorithm analyzes the distribution of range classes and selects the optimal splitting strategy to minimize overlap and ensure balanced tree growth.

## Parameters / Member Variables
- : Standard PostgreSQL function arguments containing:
  - : GistEntryVector pointer containing the range entries to be split
  - : GIST_SPLITVEC pointer for storing the split result (left and right groups)

## Dependencies
- Functions called/Symbols referenced:
  - : Convert datum to range type
  - : Get type cache for range operations
  - : Get OID of range type
  - : Classify range into categories
  - : Split normal ranges using double sorting
  - : Split ranges with one infinite bound
  - : Trivial split for special cases
  - : Perform class-based splitting
- Called from (representative examples):
  - GiST index operations (indirectly through function pointer in opclass)

## Notes and Other Information
- Located in src/backend/utils/adt/rangetypes_gist.c:619-777
- This is a critical function for GiST index performance on range types
- Uses sophisticated heuristics to balance tree structure and minimize overlap
- Handles various range class combinations (CLS_NORMAL, CLS_LOWER_INF, CLS_UPPER_INF, CLS_EMPTY, CLS_CONTAIN_EMPTY)
- The splitting strategy directly impacts query performance on range indexes

## Simplified Source

```c
Datum
range_gist_picksplit(PG_FUNCTION_ARGS)
{
	GistEntryVector *entryvec = (GistEntryVector *) PG_GETARG_POINTER(0);
	GIST_SPLITVEC *v = (GIST_SPLITVEC *) PG_GETARG_POINTER(1);
	TypeCacheEntry *typcache;
	OffsetNumber maxoff;
	int			count_in_classes[CLS_COUNT];
	int			non_empty_classes_count = 0;
	int			biggest_class = -1;
	int			biggest_class_count = 0;

	// Get type cache from first entry
	RangeType *first_range = DatumGetRangeTypeP(entryvec->vector[FirstOffsetNumber].key);
	typcache = range_get_typcache(fcinfo, RangeTypeGetOid(first_range));

	maxoff = entryvec->n - 1;
	v->spl_left = (OffsetNumber *) palloc((maxoff + 1) * sizeof(OffsetNumber));
	v->spl_right = (OffsetNumber *) palloc((maxoff + 1) * sizeof(OffsetNumber));

	// Count distribution of range classes
	memset(count_in_classes, 0, sizeof(count_in_classes));
	for (OffsetNumber i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i)) {
		RangeType *range = DatumGetRangeTypeP(entryvec->vector[i].key);
		count_in_classes[get_gist_range_class(range)]++;
	}

	// Find biggest class and count non-empty classes
	for (int j = 0; j < CLS_COUNT; j++) {
		if (count_in_classes[j] > 0) {
			if (count_in_classes[j] > biggest_class_count) {
				biggest_class_count = count_in_classes[j];
				biggest_class = j;
			}
			non_empty_classes_count++;
		}
	}

	// Choose splitting strategy
	if (non_empty_classes_count == 1) {
		// Single class: use appropriate within-class split
		if ((biggest_class & ~CLS_CONTAIN_EMPTY) == CLS_NORMAL)
			range_gist_double_sorting_split(typcache, entryvec, v);
		else if ((biggest_class & ~CLS_CONTAIN_EMPTY) == CLS_LOWER_INF)
			range_gist_single_sorting_split(typcache, entryvec, v, true);
		else if ((biggest_class & ~CLS_CONTAIN_EMPTY) == CLS_UPPER_INF)
			range_gist_single_sorting_split(typcache, entryvec, v, false);
		else
			range_gist_fallback_split(typcache, entryvec, v);
	} else {
		// Multiple classes: use class-based split
		SplitLR classes_groups[CLS_COUNT];
		memset(classes_groups, 0, sizeof(classes_groups));

		// Determine optimal class grouping
		if (count_in_classes[CLS_NORMAL] > 0) {
			classes_groups[CLS_NORMAL] = SPLIT_RIGHT;
		} else {
			// Balance by infinity or emptiness
			int infCount = maxoff - (count_in_classes[CLS_NORMAL] +
									 count_in_classes[CLS_CONTAIN_EMPTY] +
									 count_in_classes[CLS_EMPTY]);
			int nonInfCount = maxoff - infCount;
			int emptyCount = maxoff - (count_in_classes[CLS_NORMAL] +
									   count_in_classes[CLS_LOWER_INF] +
									   count_in_classes[CLS_UPPER_INF] +
									   count_in_classes[CLS_LOWER_INF | CLS_UPPER_INF]);

			if (infCount > 0 && nonInfCount > 0 &&
				(abs(infCount - nonInfCount) <= abs(emptyCount - (maxoff - emptyCount)))) {
				// Split by infinity
				classes_groups[CLS_NORMAL] = SPLIT_RIGHT;
				classes_groups[CLS_CONTAIN_EMPTY] = SPLIT_RIGHT;
				classes_groups[CLS_EMPTY] = SPLIT_RIGHT;
			} else if (emptyCount > 0 && (maxoff - emptyCount) > 0) {
				// Split by emptiness
				classes_groups[CLS_NORMAL] = SPLIT_RIGHT;
				classes_groups[CLS_LOWER_INF] = SPLIT_RIGHT;
				classes_groups[CLS_UPPER_INF] = SPLIT_RIGHT;
				classes_groups[CLS_LOWER_INF | CLS_UPPER_INF] = SPLIT_RIGHT;
			} else {
				// Fallback: separate biggest class
				classes_groups[biggest_class] = SPLIT_RIGHT;
			}
		}

		range_gist_class_split(typcache, entryvec, v, classes_groups);
	}

	PG_RETURN_POINTER(v);
}
```