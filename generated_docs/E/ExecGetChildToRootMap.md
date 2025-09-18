# ExecGetChildToRootMap

## Location
[src/backend/executor/execUtils.c:1206-1231](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execUtils.c#L1206-L1231)

## Overview
Returns a tuple conversion map that transforms tuples from a child result relation to match the rowtype of the query's main target (root) relation, computing it lazily if needed.

## Definition


## Detailed Description
This function is crucial for PostgreSQL's partitioned table support and inheritance hierarchies. When working with partitioned tables or inheritance, child relations may have different column layouts than their parent (root) relation. This function provides the mapping needed to convert tuples from a child relation's format to the root relation's format.

The function implements lazy computation with caching - it only calculates the conversion map when first requested and marks it as valid for future reuse. If no conversion is needed (when the child and root have identical layouts), it returns NULL, which is a valid and expected result.

The conversion map is essential for operations that need to present a unified view of data across partition boundaries or inheritance hierarchies, ensuring that tuples from different child relations can be properly converted to the expected root relation format.

## Parameters / Member Variables
- : Result relation info structure for the child relation, which contains references to the root relation and caching fields for the conversion map

## Dependencies
- Functions called/Symbols referenced:
  -  (creates tuple conversion map by matching column names between source and target descriptors)
  -  (return type representing the conversion mapping)
- Called from (representative examples):
  -  (src/backend/commands/trigger.c:4468, 4511)
  -  (src/backend/commands/trigger.c:5601)
  -  (src/backend/commands/trigger.c:6373, 6382)
  -  (src/backend/executor/execPartition.c:1701)
  -  (src/backend/executor/nodeModifyTable.c:1894)

## Notes and Other Information
- Uses lazy computation with caching via  flag
- Returns NULL when no conversion is needed, which is a valid result indicating identical tuple layouts
- Essential for PostgreSQL's partitioned table functionality and inheritance support
- The conversion map is computed by matching columns by name using 
- Cached in  for performance across multiple tuple conversions
- Critical for maintaining data consistency across partition boundaries in UPDATE operations
- Used extensively in trigger processing where tuples may need to be converted between child and root relation formats
- Part of the infrastructure that makes partitioned tables transparent to upper-level query processing