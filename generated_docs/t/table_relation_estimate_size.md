# table_relation_estimate_size

## Location
[src/include/access/tableam.h:1938-1961](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/tableam.h#L1938-L1961)

## Overview
Estimates the current size and statistics of a relation, serving as an access method-specific implementation for the query planner's size estimation needs.

## Definition


## Detailed Description
This function provides a table access method interface for estimating relation size and statistics that are crucial for query planning and optimization. It serves as the access method-specific workhorse for the higher-level estimate_rel_size() function. The function delegates to the underlying table access method's relation_estimate_size function, allowing different storage engines to implement their own size estimation algorithms based on their specific storage characteristics and optimization strategies.

The estimates provided by this function are used by PostgreSQL's query planner to make informed decisions about join strategies, index usage, and other optimization choices. Different access methods may have varying approaches to calculating these estimates based on their internal metadata and storage organization.

## Parameters / Member Variables
- : A Relation pointer representing the table relation whose size is being estimated
- : Array of int32 values representing the average width of each attribute (input parameter for estimation)
- : Pointer to BlockNumber where the estimated number of pages will be stored (output parameter)
- : Pointer to double where the estimated number of tuples will be stored (output parameter)
- : Pointer to double where the estimated fraction of all-visible pages will be stored (output parameter)

## Dependencies
- Functions called/Symbols referenced:
  - rel->rd_tableam->relation_estimate_size (table access method function pointer)
- Called from (representative examples):
  - [estimate_rel_size](../e/estimate_rel_size.md) (in src/backend/optimizer/util/plancat.c:1071)

## Notes and Other Information
- This is an inline function defined in the tableam header file for efficient access
- Part of the table access method abstraction layer serving the query planner
- Critical component of PostgreSQL's cost-based optimization system
- Provides estimates rather than exact counts for performance reasons during planning
- The allvisfrac parameter relates to visibility map optimization in PostgreSQL
- Different access methods may use different algorithms and metadata for size estimation
- Located in src/include/access/tableam.h:1938-1961