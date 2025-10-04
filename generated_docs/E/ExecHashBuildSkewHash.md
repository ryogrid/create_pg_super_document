# ExecHashBuildSkewHash

## Location
[src/backend/executor/nodeHash.c:2382-2534](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHash.c#L2382-L2534)

## Overview
Sets up skew optimization for hash joins by creating specialized hash buckets for the most common values (MCVs) of the outer relation's join key to improve hash table performance.

## Definition

```c
static void
ExecHashBuildSkewHash(HashJoinTable hashtable, Hash *node, int mcvsToUse)
```
## Detailed Description
ExecHashBuildSkewHash implements a performance optimization technique for hash joins called "skew optimization." When the outer relation's join key has highly skewed data distribution (some values appear much more frequently than others), normal hash table performance can degrade due to bucket collisions and uneven distribution.

This function creates a separate skew hash table with dedicated buckets for the most common values (MCVs) identified by the query planner. The skew hash table uses open addressing with power-of-2 sizing and is allocated in the hashtable's batch context for automatic cleanup.

The function retrieves statistics from the system catalog (pg_statistic) to identify MCVs and their frequencies. It only proceeds if the total frequency of MCVs exceeds SKEW_MIN_OUTER_FRACTION to ensure the optimization is worthwhile. Skew buckets are created in order of decreasing MCV frequency, which is important for proper bucket removal during memory pressure.

## Parameters / Member Variables
- `hashtable`: The HashJoinTable structure being optimized with skew buckets
- `*node`: Hash node containing skew optimization metadata (skewTable, skewColumn, skewInherit)
- `mcvsToUse`: Maximum number of MCV values to create skew buckets for, based on available memory
## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache3](../S/SearchSysCache3.md) (retrieves statistics from pg_statistic)
  - [get_attstatsslot](../g/get_attstatsslot.md) (extracts MCV values and frequencies)
  - [pg_nextpower2_32](../p/pg_nextpower2_32.md) (calculates optimal hash table size)
  - [MemoryContextAllocZero](../M/MemoryContextAllocZero.md) (allocates skew bucket arrays)
  - [FunctionCall1Coll](../F/FunctionCall1Coll.md) (computes hash values for MCVs)
  - [free_attstatsslot](../f/free_attstatsslot.md) (releases statistics slot)
- Called from:
  - [ExecHashTableCreate](ExecHashTableCreate.md) (during hash table initialization)

## Notes and Other Information
- Only activates when the planner has identified a valid skewTable OID
- Requires sufficient memory to allocate at least one skew bucket
- Uses open addressing hash table with power-of-2 sizing plus extra bits to reduce collisions
- Memory allocation occurs in batch context for automatic cleanup after first batch
- Critical that skew buckets are created in decreasing MCV frequency order for proper removal during memory pressure
- Skew optimization is abandoned if MCV frequency sum is below SKEW_MIN_OUTER_FRACTION threshold
- Handles hash collisions between different MCVs by allowing bucket sharing

## Simplified Source

```c
static void
ExecHashBuildSkewHash(HashJoinTable hashtable, Hash *node, int mcvsToUse)
{
    HeapTupleData *statsTuple;
    AttStatsSlot sslot;

    // Early exits: no skew table identified or insufficient memory
    if (!OidIsValid(node->skewTable) || mcvsToUse <= 0)
        return;

    // Find MCV statistics for the outer relation's join key
    statsTuple = SearchSysCache3(STATRELATTINH,
                                ObjectIdGetDatum(node->skewTable),
                                Int16GetDatum(node->skewColumn),
                                BoolGetDatum(node->skewInherit));
    if (!HeapTupleIsValid(statsTuple))
        return;

    // Extract MCV values and frequencies from statistics
    if (get_attstatsslot(&sslot, statsTuple,
                        STATISTIC_KIND_MCV, InvalidOid,
                        ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS))
    {
        double frac = 0;
        int nbuckets;
        FmgrInfo *hashfunctions;
        int i;

        // Limit MCVs to available values
        if (mcvsToUse > sslot.nvalues)
            mcvsToUse = sslot.nvalues;

        // Calculate total frequency of MCVs to use
        for (i = 0; i < mcvsToUse; i++)
            frac += sslot.numbers[i];

        // Skip optimization if frequency too low
        if (frac < SKEW_MIN_OUTER_FRACTION)
        {
            free_attstatsslot(&sslot);
            ReleaseSysCache(statsTuple);
            return;
        }

        // Set up skew hashtable with power-of-2 size plus extra bits
        nbuckets = pg_nextpower2_32(mcvsToUse + 1) << 2;

        hashtable->skewEnabled = true;
        hashtable->skewBucketLen = nbuckets;

        // Allocate bucket arrays in batch context
        hashtable->skewBucket = (HashSkewBucket **)
            MemoryContextAllocZero(hashtable->batchCxt,
                                  nbuckets * sizeof(HashSkewBucket *));
        hashtable->skewBucketNums = (int *)
            MemoryContextAllocZero(hashtable->batchCxt,
                                  mcvsToUse * sizeof(int));

        // Update memory usage tracking
        hashtable->spaceUsed += nbuckets * sizeof(HashSkewBucket *) +
                               mcvsToUse * sizeof(int);
        hashtable->spaceUsedSkew += nbuckets * sizeof(HashSkewBucket *) +
                                   mcvsToUse * sizeof(int);
        if (hashtable->spaceUsed > hashtable->spacePeak)
            hashtable->spacePeak = hashtable->spaceUsed;

        // Create skew buckets for each MCV (in decreasing frequency order)
        hashfunctions = hashtable->outer_hashfunctions;

        for (i = 0; i < mcvsToUse; i++)
        {
            uint32 hashvalue;
            int bucket;

            // Compute hash value for this MCV
            hashvalue = DatumGetUInt32(FunctionCall1Coll(&hashfunctions[0],
                                                        hashtable->collations[0],
                                                        sslot.values[i]));

            // Find empty bucket using linear probing
            bucket = hashvalue & (nbuckets - 1);
            while (hashtable->skewBucket[bucket] != NULL &&
                   hashtable->skewBucket[bucket]->hashvalue != hashvalue)
                bucket = (bucket + 1) & (nbuckets - 1);

            // Skip if bucket already exists for this hash value
            if (hashtable->skewBucket[bucket] != NULL)
                continue;

            // Create new skew bucket
            hashtable->skewBucket[bucket] = (HashSkewBucket *)
                MemoryContextAlloc(hashtable->batchCxt,
                                  sizeof(HashSkewBucket));
            hashtable->skewBucket[bucket]->hashvalue = hashvalue;
            hashtable->skewBucket[bucket]->tuples = NULL;
            hashtable->skewBucketNums[hashtable->nSkewBuckets] = bucket;
            hashtable->nSkewBuckets++;

            // Update memory usage
            hashtable->spaceUsed += SKEW_BUCKET_OVERHEAD;
            hashtable->spaceUsedSkew += SKEW_BUCKET_OVERHEAD;
            if (hashtable->spaceUsed > hashtable->spacePeak)
                hashtable->spacePeak = hashtable->spaceUsed;
        }

        free_attstatsslot(&sslot);
    }

    ReleaseSysCache(statsTuple);
}
```