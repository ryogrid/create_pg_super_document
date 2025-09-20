# range_serialize

## Location
[src/backend/utils/adt/rangetypes.c:1727-1855](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L1727-L1855)

## Overview
Constructs a range value from bound specifications and empty flag, performing validation and serialization into the internal RangeType format.

## Definition

```c
struct Node *escontext)
{
	RangeType  *range;
	int			cmp;
	Size		msize;
	Pointer		ptr;
	int16		typlen;
	bool		typbyval;
	char		typalign;
	char		typstorage;
	char		flags = 0;

	/*
	 * Verify range is not invalid on its face, and construct flags value,
	 * preventing any non-canonical combinations such as infinite+inclusive.
	 */
	Assert(lower->lower);
	Assert(!upper->lower);

	if (empty)
		flags |= RANGE_EMPTY;
	else
	{
		cmp = range_cmp_bound_values(typcache, lower, upper);

		/* error check: if lower bound value is above upper, it's wrong */
		if (cmp > 0)
			ereturn(escontext, NULL,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("range lower bound must be less than or equal to range upper bound")));

		/* if bounds are equal, and not both inclusive, range is empty */
		if (cmp == 0 && !(lower->inclusive && upper->inclusive))
			flags |= RANGE_EMPTY;
		else
		{
			/* infinite boundaries are never inclusive */
			if (lower->infinite)
				flags |= RANGE_LB_INF;
			else if (lower->inclusive)
				flags |= RANGE_LB_INC;
			if (upper->infinite)
				flags |= RANGE_UB_INF;
			else if (upper->inclusive)
				flags |= RANGE_UB_INC;
		}
	}

	/* Fetch information about range's element type */
	typlen = typcache->rngelemtype->typlen;
	typbyval = typcache->rngelemtype->typbyval;
	typalign = typcache->rngelemtype->typalign;
	typstorage = typcache->rngelemtype->typstorage;

	/* Count space for varlena header and range type's OID */
	msize = sizeof(RangeType);
	Assert(msize == MAXALIGN(msize));

	/* Count space for bounds */
	if (RANGE_HAS_LBOUND(flags))
	{
		/*
		 * Make sure item to be inserted is not toasted.  It is essential that
		 * we not insert an out-of-line toast value pointer into a range
		 * object, for the same reasons that arrays and records can't contain
		 * them.  It would work to store a compressed-in-line value, but we
		 * prefer to decompress and then let compression be applied to the
		 * whole range object if necessary.  But, unlike arrays, we do allow
		 * short-header varlena objects to stay as-is.
		 */
		if (typlen == -1)
			lower->val = PointerGetDatum(PG_DETOAST_DATUM_PACKED(lower->val));

		msize = datum_compute_size(msize, lower->val, typbyval, typalign,
								   typlen, typstorage);
	}

	if (RANGE_HAS_UBOUND(flags))
	{
		/* Make sure item to be inserted is not toasted */
		if (typlen == -1)
			upper->val = PointerGetDatum(PG_DETOAST_DATUM_PACKED(upper->val));

		msize = datum_compute_size(msize, upper->val, typbyval, typalign,
								   typlen, typstorage);
	}

	/* Add space for flag byte */
	msize += sizeof(char);

	/* Note: zero-fill is required here, just as in heap tuples */
	range = (RangeType *) palloc0(msize);
	SET_VARSIZE(range, msize);

	/* Now fill in the datum */
	range->rangetypid = typcache->type_id;

	ptr = (char *) (range + 1);

	if (RANGE_HAS_LBOUND(flags))
	{
		Assert(lower->lower);
		ptr = datum_write(ptr, lower->val, typbyval, typalign, typlen,
						  typstorage);
	}

	if (RANGE_HAS_UBOUND(flags))
	{
		Assert(!upper->lower);
		ptr = datum_write(ptr, upper->val, typbyval, typalign, typlen,
						  typstorage);
	}

	*((char *) ptr) = flags;

	return range;
}

/*
 * range_deserialize: deconstruct a range value
 *
 * NB: the given range object must be fully detoasted;
```
## Detailed Description
This function creates a properly serialized RangeType object from bound specifications. It performs comprehensive validation including checking that lower bounds are not greater than upper bounds, handling infinite and inclusive boundary flags, and ensuring proper canonicalization. The function constructs the internal binary representation by calculating the required storage size, handling TOAST decompression for variable-length data types, and writing the bounds and flags in the correct format. It's primarily intended for use by canonicalization functions and internal range operations.

## Parameters
- `typcache`: Type cache entry containing metadata about the range type and its element type
- `lower`: Lower bound specification with value, inclusivity, and infinity flags
- `upper`: Upper bound specification with value, inclusivity, and infinity flags  
- `empty`: Boolean flag indicating if the range should be empty
- `escontext`: Error context for soft error handling

## Dependencies
- Functions called/Symbols referenced:
  - [range_cmp_bound_values](range_cmp_bound_values.md)
  - PG_DETOAST_DATUM_PACKED
  - [datum_compute_size](../d/datum_compute_size.md)
  - [datum_write](../d/datum_write.md)
  - SET_VARSIZE
  - RANGE_EMPTY, RANGE_LB_INF, RANGE_LB_INC, RANGE_UB_INF, RANGE_UB_INC
  - RANGE_HAS_LBOUND, RANGE_HAS_UBOUND
- Called from (representative examples):
  - [int4range_canonical](../i/int4range_canonical.md)
  - [int8range_canonical](../i/int8range_canonical.md)
  - [daterange_canonical](../d/daterange_canonical.md)
  - [make_range](../m/make_range.md)
  - [rangesel](rangesel.md)
  - [compute_range_stats](../c/compute_range_stats.md)

## Notes and Other Information
- Does not force canonicalization of the range value - that's left to caller functions
- Performs datatype-independent canonicalization checks for safety
- Handles TOAST values by decompressing them to avoid storing out-of-line pointers
- Supports soft error handling through the escontext parameter
- The serialized format includes varlena header, range type OID, bounds data, and flags byte
- Zero-fills allocated memory similar to heap tuples for consistency