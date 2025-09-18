# network_abbrev_abort

## Location
[src/backend/utils/adt/network.c:488-624](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network.c#L488-L624)

## Overview
Callback function that estimates the effectiveness of abbreviated key optimization during sorting of network data types, determining whether to abort the abbreviation strategy based on cardinality analysis.

## Definition


## Detailed Description
This function is a key component of PostgreSQL's sort support optimization system for network data types (inet/cidr). It implements adaptive logic to determine whether abbreviated sorting should be abandoned in favor of authoritative comparison. The function uses HyperLogLog cardinality estimation to analyze the distinctness of abbreviated keys and makes intelligent decisions about continuing or aborting the abbreviation optimization.

The algorithm considers three main scenarios:
1. **Early termination conditions**: Returns false if insufficient data samples (< 10k tuples) or estimation is disabled
2. **High cardinality optimization**: Commits fully to abbreviation when distinct values exceed 100k, stopping further estimation
3. **Low cardinality abort condition**: Aborts abbreviation when cardinality falls below the threshold of approximately 1 distinct value per 2000 input rows

The function includes comprehensive tracing support for debugging sort performance issues when TRACE_SORT is enabled.

## Parameters / Member Variables
- : Number of tuples currently in memory during sorting operation
- : SortSupport structure containing sorting context and abbreviated key state information

## Dependencies
- Functions called/Symbols referenced:
  - : Estimates cardinality of abbreviated keys using HyperLogLog algorithm
  - : Logging function for trace output when TRACE_SORT is enabled
  - : Sort support framework structure type
  - : Network-specific sort support state structure

- Called from (representative examples):
  - : Sets this function as the abort callback in sort support initialization

## Notes and Other Information
- This is a static function internal to the network.c module, not exposed publicly
- The cardinality threshold calculation (input_count / 2000.0 + 0.5) includes a fudge factor to handle pathological cases
- The 100k distinct value threshold represents a commitment point where abbreviation is deemed definitively beneficial
- Trace logging is conditionally compiled and only active when both TRACE_SORT is defined and trace_sort is enabled
- The function is designed to work with PostgreSQL's adaptive sorting infrastructure, balancing memory usage against comparison speed