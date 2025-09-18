# clauselist_selectivity_ext

## Location
src/backend/optimizer/path/clausesel.c: 117 - 358

## Overview
Extended version of clauselist_selectivity that provides fine-grained control over extended statistics usage and implements sophisticated selectivity estimation for ANDed clauses, including range query optimization and extended statistics correlation analysis.

## Definition
```c
Selectivity clauselist_selectivity_ext(PlannerInfo *root,
                                       List *clauses,
                                       int varRelid,
                                       JoinType jointype,
                                       SpecialJoinInfo *sjinfo,
                                       bool use_extended_stats)
```

## Detailed Description
This is the core implementation of PostgreSQL's clause selectivity estimation algorithm. The function processes a list of ANDed boolean clauses using multiple sophisticated techniques:

1. **Single Clause Optimization**: For single-clause lists, delegates directly to `clause_selectivity_ext` to avoid unnecessary overhead.

2. **Extended Statistics Application**: When `use_extended_stats` is true and clauses reference a single relation with extended statistics, applies cross-column correlation analysis via `statext_clauselist_selectivity`.

3. **Range Query Recognition**: Identifies and optimizes range queries (e.g., "x > 34 AND x < 42") by:
   - Recognizing restriction opclauses with scalar comparison operators
   - Pairing clauses that reference the same variable 
   - Using the formula: hisel + losel - 1 + null_frac instead of hisel * losel
   - Handling redundant inequalities by keeping only the tighter constraint

4. **Fallback Processing**: For unmatched clauses, applies standard independence assumption (multiplication of selectivities).

5. **Robustness Features**: Includes safeguards for default selectivity estimates, roundoff errors, and impossible negative results.

The algorithm significantly improves selectivity estimation accuracy compared to naive independence assumptions, especially for correlated columns and range predicates.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing query planning context, relation statistics, and metadata
- `clauses`: List of boolean expression clauses to be ANDed together (can be empty or single-element)
- `varRelid`: Variable relation ID for parameterized path estimation context
- `jointype`: Join type affecting selectivity calculation methodology
- `sjinfo`: Special join information for outer join and semi-join handling
- `use_extended_stats`: Boolean flag to enable/disable extended statistics usage for cross-column correlations

## Dependencies
- Functions called/Symbols referenced:
  - clause_selectivity_ext
  - find_single_rel_for_clauses
  - statext_clauselist_selectivity
  - addRangeClause
  - bms_is_member
  - is_opclause
  - is_pseudo_constant_clause_relids
  - is_pseudo_constant_clause
  - get_oprrest
  - nulltestsel
  - NumRelids
  - RTE_RELATION
  - DEFAULT_INEQ_SEL
  - DEFAULT_RANGE_INEQ_SEL
  - IS_NULL
- Called from (representative examples):
  - clauselist_selectivity
  - clause_selectivity_ext
  - clauselist_apply_dependencies
  - statext_mcv_clauselist_selectivity

## Notes and Other Information
This function represents the heart of PostgreSQL's selectivity estimation engine and is critical for accurate cost-based query optimization. Key implementation notes:

- Range query optimization uses the insight that for "x > a AND x < b", the selectivity is the overlap of ranges rather than their product
- Extended statistics integration allows capturing real-world column correlations that independence assumptions miss
- The algorithm maintains a range query list (rqlist) to track and pair comparison operators
- Handles edge cases like pseudoconstant clauses, default selectivity punting, and numerical precision issues
- Performance optimization includes early single-clause detection and bitmap-based clause tracking
- The function is extensively used throughout the optimizer for cost estimation and plan selection