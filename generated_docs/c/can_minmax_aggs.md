# can_minmax_aggs

## Location
[src/backend/optimizer/plan/planagg.c:236-315](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planagg.c#L236-L315)

## Overview
Examines all aggregates in a query to verify they are MIN/MAX aggregates and builds a list of MinMaxAggInfo nodes for optimization planning.

## Definition


## Detailed Description
This function validates whether all aggregates in a query are eligible for MIN/MAX optimization by examining each aggregate through the following criteria:

1. **Aggregate Structure**: Must have exactly one argument (single-column aggregates)
2. **Order Independence**: Rejects aggregates with ORDER BY clauses, as these can affect results when operator classes recognize non-identical values as equal
3. **Filter Absence**: Currently rejects aggregates with FILTER clauses (future enhancement possibility)
4. **MIN/MAX Verification**: Uses  to confirm the aggregate has a sort operator (indicating it's MIN or MAX)
5. **Mutability Check**: Ensures the aggregate argument doesn't contain mutable functions that would prevent indexable access
6. **Type Validation**: Rejects row-type expressions due to complex IS NOT NULL semantics

For each valid aggregate, it creates a  node containing the aggregate's function OID, sort operator, target expression, and placeholder fields for later path planning.

## Parameters / Member Variables
- : PlannerInfo structure containing query planning context and aggregate information
- : Output parameter - pointer to a list that will be populated with MinMaxAggInfo nodes for valid aggregates

## Dependencies
- Functions called/Symbols referenced:
  -  - Retrieves the sort operator for an aggregate function
  -  - Checks if expression contains non-stable functions
  -  - Determines if expression type is a row/composite type
  -  - Creates new MinMaxAggInfo nodes
- Called from (representative examples):
  -  (src/backend/optimizer/plan/planagg.c:143)

## Notes and Other Information
- Returns false if any aggregate is not eligible for MIN/MAX optimization, true if all are eligible
- Uses the AggInfo list created by  rather than scanning the query directly
- DISTINCT clauses in aggregates are ignored (don't affect optimization eligibility)
- Future enhancement could support FILTER clauses by adding them to generated subquery quals
- The ORDER BY restriction prevents optimization of aggregates where result order matters for equal values