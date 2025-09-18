# hash_choose_num_buckets

## Location
[src/backend/executor/nodeAgg.c:1966-1990](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeAgg.c#L1966-L1990)

## Overview
Calculates an appropriate number of buckets for the initial hash table size in hash aggregation, balancing memory usage with hash distribution efficiency.

## Definition


## Detailed Description
This function determines the optimal number of buckets for a hash table used in aggregation operations. It takes a conservative approach by starting with the expected number of groups and then limiting this based on available memory and estimated hash entry size. The function deliberately underestimates rather than overestimates to prevent crowding out space needed for group keys and transition state values. The calculated maximum is halved to ensure sufficient memory remains for the actual data storage.

## Parameters / Member Variables
- : The estimated size in bytes of each hash table entry
- : The estimated number of groups expected in the aggregation
- : The total amount of memory available for the hash table

## Dependencies
- Functions called/Symbols referenced:
  - Max (macro for maximum value)
- Called from (representative examples):
  - [build_hash_tables](../b/build_hash_tables.md)

## Notes and Other Information
- The function implements a conservative strategy, preferring to underestimate bucket count rather than risk memory exhaustion
- Maximum bucket count is deliberately halved (>>= 1) to reserve space for actual hash entry data
- Returns at least 1 bucket to ensure the hash table can function
- The balance between bucket count and memory usage is crucial for hash aggregation performance
- Too many buckets reduce available space for data; too few buckets increase hash collisions