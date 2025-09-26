# statext_ndistinct_build

## Location
src/backend/statistics/mvdistinct.c: 88 - 147

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
  - num_combinations: Calculates total number of attribute combinations
  - generator_init: Initializes combination generator
  - generator_next: Gets next attribute combination
  - generator_free: Frees combination generator resources
  - ndistinct_for_combination: Computes ndistinct estimate for specific combination
  - AttributeNumberIsValid: Validates attribute numbers
  - palloc: PostgreSQL memory allocation
- Called from (representative examples):
  - BuildRelationExtStatistics: Main extended statistics building function

## Notes and Other Information
- Only generates combinations of 2 or more attributes (k >= 2)
- Uses systematic enumeration of all possible combinations rather than sampling
- Memory allocation includes space for the variable-length items array
- Results are stored with STATS_NDISTINCT_MAGIC and STATS_NDISTINCT_TYPE_BASIC identifiers
- The function ensures exact consumption of the allocated output array through assertions
- Part of PostgreSQL's multivariate statistics infrastructure for query optimization