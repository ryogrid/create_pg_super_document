# statext_ndistinct_build

## Location
[src/backend/statistics/mvdistinct.c:88-147](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/mvdistinct.c#L88-L147)

## Overview
Computes ndistinct coefficients for all possible combinations of attributes in a multivariate statistics object, using the same estimator used in analyze.c.

## Definition

```c
MVNDistinct *
statext_ndistinct_build(double totalrows, StatsBuildData *data)
```
## Detailed Description
This function builds a complete MVNDistinct structure containing ndistinct estimates for all possible combinations of 2 or more attributes from the provided attribute set. It uses a combination generator to systematically create all possible attribute combinations and computes the ndistinct estimate for each combination using the same estimator employed in PostgreSQL's ANALYZE command.

The function handles expressions by treating them as system attributes with negative attribute numbers, offsetting everything by the number of expressions to allow using Bitmapsets for efficient processing.

The resulting structure contains:
- Magic number and type identification
- Array of MVNDistinctItem structures, each containing:
  - Set of attributes in the combination
  - Computed ndistinct estimate for that combination

## Parameters / Member Variables
- : Total number of rows in the relation being analyzed
- : StatsBuildData structure containing attribute information and sample data for computing statistics

## Dependencies
- Functions called/Symbols referenced:
  - [num_combinations](../n/num_combinations.md): Calculates total number of attribute combinations
  - [generator_init](../g/generator_init.md): Initializes combination generator
  - [generator_next](../g/generator_next.md): Gets next attribute combination
  - [generator_free](../g/generator_free.md): Frees combination generator resources
  - [ndistinct_for_combination](../n/ndistinct_for_combination.md): Computes ndistinct estimate for specific combination
  - AttributeNumberIsValid: Validates attribute numbers
  - [palloc](../p/palloc.md): PostgreSQL memory allocation
- Called from (representative examples):
  - [BuildRelationExtStatistics](../B/BuildRelationExtStatistics.md): Main extended statistics building function

## Notes and Other Information
- Only generates combinations of 2 or more attributes (k >= 2)
- Uses systematic enumeration of all possible combinations rather than sampling
- Memory allocation includes space for the variable-length items array
- Results are stored with STATS_NDISTINCT_MAGIC and STATS_NDISTINCT_TYPE_BASIC identifiers
- The function ensures exact consumption of the allocated output array through assertions
- Part of PostgreSQL's multivariate statistics infrastructure for query optimization

## Simplified Source

```c
MVNDistinct *
statext_ndistinct_build(double totalrows, StatsBuildData *data)
{
    MVNDistinct *result;
    int numattrs = data->nattnums;
    int numcombs = num_combinations(numattrs);
    int itemcnt = 0;

    // Allocate result structure with space for all combinations
    result = palloc(offsetof(MVNDistinct, items) +
                   numcombs * sizeof(MVNDistinctItem));
    result->magic = STATS_NDISTINCT_MAGIC;
    result->type = STATS_NDISTINCT_TYPE_BASIC;
    result->nitems = numcombs;

    // Generate all combinations from size 2 up to all attributes
    for (int k = 2; k <= numattrs; k++)
    {
        int *combination;
        CombinationGenerator *generator;

        // Generate all combinations of k attributes
        generator = generator_init(numattrs, k);

        while ((combination = generator_next(generator)))
        {
            MVNDistinctItem *item = &result->items[itemcnt];

            // Allocate and populate attribute array
            item->attributes = palloc(sizeof(AttrNumber) * k);
            item->nattributes = k;

            // Translate indexes to actual attribute numbers
            for (int j = 0; j < k; j++)
            {
                item->attributes[j] = data->attnums[combination[j]];
                Assert(AttributeNumberIsValid(item->attributes[j]));
            }

            // Compute ndistinct estimate for this combination
            item->ndistinct = ndistinct_for_combination(totalrows, data, k, combination);

            itemcnt++;
            Assert(itemcnt <= result->nitems);
        }

        generator_free(generator);
    }

    // Verify we filled the entire output array
    Assert(itemcnt == result->nitems);

    return result;
}
```