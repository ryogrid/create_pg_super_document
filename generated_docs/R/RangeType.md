# RangeType

## Location
[src/include/utils/rangetypes.h:30-31](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/rangetypes.h#L30-L31)

## Overview
RangeType is a fundamental data structure in PostgreSQL that represents range types - values that represent a range of data values of some element type.

## Definition

```c
typedef struct
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	Oid			rangetypid;		/* range type's own OID */
	/* Following the OID are zero to two bound values, then a flags byte */
} RangeType;
```
## Detailed Description
RangeType is the core structure for PostgreSQL's range types system, which allows representation of continuous ranges of values such as integer ranges, timestamp ranges, etc. As a varlena object, it follows PostgreSQL's variable-length data convention where the first int32 contains the total object size.

The structure stores the essential metadata (type OID) followed by variable-length data containing the actual range bounds and flags. The range can be empty, have one or two bounds, and each bound can be inclusive/exclusive or infinite. The physical layout optimizes storage by placing bounds and flags after the fixed header.

Range types support various operations like overlap detection, containment checks, union, intersection, and adjacency testing, making them useful for temporal data, numeric ranges, and other continuous data domains.

## Parameters / Member Variables
- `vl_len_`: Standard varlena header containing the total object size in bytes (use VARSIZE()/SET_VARSIZE() macros)
- `rangetypid`: Object identifier (OID) of the specific range type (e.g., int4range, tsrange, etc.)
## Dependencies
- Functions called/Symbols referenced:
  - VARSIZE macro (for varlena size access)
  - SET_VARSIZE macro (for varlena size setting)
  - PG_DETOAST_DATUM (via DatumGetRangeTypeP)
- Called from (representative examples):
  - [range_deserialize](../r/range_deserialize.md) (extracts bounds from RangeType)
  - [range_serialize](../r/range_serialize.md) (creates RangeType from bounds)
  - Various range operation functions (range_eq_internal, range_contains_internal, etc.)

## Notes and Other Information
- Range data follows the struct: bounds are stored after the OID, followed by a flags byte
- Use RangeTypeGetOid(r) macro instead of direct rangetypid field access
- Flags byte encodes range properties: RANGE_EMPTY, RANGE_LB_INC, RANGE_UB_INC, RANGE_LB_INF, RANGE_UB_INF
- Related helper structure RangeBound represents individual bounds during processing
- Supports GiST and SP-GiST indexing with specialized strategy numbers
- Part of PostgreSQL's extensible type system allowing custom range types