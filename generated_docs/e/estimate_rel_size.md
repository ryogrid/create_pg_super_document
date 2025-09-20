# estimate_rel_size

## Location
[src/backend/optimizer/util/plancat.c:1060-1184](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/plancat.c#L1060-L1184)

## Overview
Estimates the number of pages and tuples in a table or index, along with the fraction of all-visible pages for index-only scan optimization.

## Definition

```c
structure.  Currently it's OK for btree, hash, and GIN indexes but
		 * suspect for GiST indexes.
		 */
		if (relpages > 0)
		{
			curpages--;
			relpages--;
		}

		/* estimate number of tuples from previous tuple density */
		if (reltuples >= 0 && relpages > 0)
			density = reltuples / (double) relpages;
```
## Detailed Description
This function provides size estimates for relations by analyzing their storage characteristics and statistical metadata. It handles different relation types with specialized logic:

For tables with table access methods, it delegates to table_relation_estimate_size for AM-specific estimation. For indexes, it performs detailed analysis including metapage adjustments and tuple density calculations. For other relation types (foreign tables, sequences), it uses pg_class metadata directly.

The index estimation process involves:
1. Getting the current physical block count via RelationGetNumberOfBlocks
2. Adjusting for metapages (subtracting one page for btree, hash, and GIN indexes)
3. Calculating tuple density from pg_class statistics or attribute width analysis
4. Estimating tuples based on density and current pages
5. Computing all-visible fraction for index-only scan cost estimation

When statistical data is unavailable (e.g., never vacuumed), the function estimates tuple width from attribute datatypes assuming fully packed pages.

## Parameters / Member Variables
- : Open Relation structure for the relation to estimate
- : Optional pointer to attribute width cache array to populate during estimation
- : Output parameter for estimated number of pages in the relation
- : Output parameter for estimated number of tuples in the relation  
- : Output parameter for fraction of all-visible pages (0.0 to 1.0)

## Dependencies
- Functions called/Symbols referenced:
  - [table_relation_estimate_size](../t/table_relation_estimate_size.md)
  - RelationGetNumberOfBlocks
  - [get_rel_data_width](../g/get_rel_data_width.md)
  - MAXALIGN, rint
  - RELKIND_HAS_TABLE_AM, RELKIND_INDEX
- Called from (representative examples):
  - [get_relation_info](../g/get_relation_info.md)
  - [hashbuild](../h/hashbuild.md)
  - [plan_create_index_workers](../p/plan_create_index_workers.md)

## Notes and Other Information
- Handles metapage discounting for index size estimation (works for btree, hash, GIN; suspect for GiST)
- Uses conservative approach for all-visible fraction - doesn't scale up like page/tuple counts
- Falls back to attribute width calculation when pg_class statistics are unavailable
- Intentionally ignores alignment considerations in width estimation for platform independence
- Foreign tables receive direct pg_class values (FDW must handle reltuples = -1)
- Zero pages results in immediate return with zero tuples and all-visible fraction