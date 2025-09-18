# compute_partition_hash_value

## Location
[src/backend/partitioning/partbounds.c:4722-4770](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partbounds.c#L4722-L4770)

## Overview
Computes a 64-bit hash value for given partition key values used in hash partitioning to determine which partition a tuple belongs to.

## Definition


## Detailed Description
This function calculates a hash value for a set of partition key values by iterating through each partition attribute and calling the appropriate datatype-specific hash function. The function combines individual hash values using the  function to produce a single 64-bit hash value that determines which hash partition a tuple should be placed in.

The function ignores NULL values during hash computation, meaning tuples with NULL values in partition key columns will only be hashed based on their non-NULL partition key values. Each partition attribute is hashed using its specific hash function with a consistent seed value () to ensure deterministic results.

## Parameters / Member Variables
- : Number of partition key attributes to process
- : Array of FmgrInfo structures containing the hash functions for each partition key attribute
- : Array of collation OIDs for each partition key attribute (used for collation-sensitive hash functions)
- : Array of Datum values representing the partition key values to hash
- : Array of boolean flags indicating which partition key values are NULL

## Dependencies
- Functions called/Symbols referenced:
  - HASH_PARTITION_SEED
  - [UInt64GetDatum](../U/UInt64GetDatum.md)
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md)
  - [hash_combine64](../h/hash_combine64.md)
  - [DatumGetUInt64](../D/DatumGetUInt64.md)
- Called from (representative examples):
  - [get_partition_for_tuple](../g/get_partition_for_tuple.md)
  - [get_matching_hash_bounds](../g/get_matching_hash_bounds.md)

## Notes and Other Information
- The function uses a fixed seed value (HASH_PARTITION_SEED) to ensure consistent hash values across different sessions and installations
- NULL values are completely ignored in hash computation, which means tuples with different NULL patterns but same non-NULL values will hash to the same partition
- The hash combination strategy ensures good distribution across partitions while maintaining deterministic behavior
- This function is critical for hash partitioning functionality in PostgreSQL's partitioning system