# _hash_get_newbucket_from_oldbucket

## Location
[src/backend/access/hash/hashutil.c:494-535](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashutil.c#L494-L535)

## Overview
Calculates the new bucket number that will be generated after splitting from a given old bucket during hash table expansion.

## Definition
Bucket _hash_get_newbucket_from_oldbucket(Relation rel, Bucket old_bucket, uint32 lowmask, uint32 maxbucket)

## Detailed Description
This function determines which new bucket number corresponds to an old bucket after a split operation in a hash index. The new bucket is calculated by OR'ing the old bucket number with the most significant bit of the current table half (identified using the lowmask parameter).

When multiple buckets could potentially have been split from the current old bucket, this function returns the first valid new bucket that exists within the current table boundaries. The algorithm first attempts to calculate the new bucket using the current lowmask. If the resulting bucket number exceeds maxbucket (indicating it's beyond the current table size), the function shifts the lowmask right by one bit and recalculates to find a valid bucket within the table bounds.

The caller must ensure that no more than one split has occurred from the old bucket to guarantee deterministic results.

## Parameters / Member Variables
- `rel`: The hash index relation (used for interface consistency but not actively used in computation)
- `old_bucket`: The bucket number of the original bucket being split
- `lowmask`: Mask representing the current table half, used to identify the MSB for new bucket calculation
- `maxbucket`: The maximum valid bucket number in the current table configuration

## Dependencies
- Functions called/Symbols referenced:
  - CALC_NEW_BUCKET (macro for calculating new bucket by OR'ing with MSB)
- Referenced types:
  - Bucket
- Called from (representative examples):
  - [hashbucketcleanup](hashbucketcleanup.md)
  - [_hash_get_newblock_from_oldbucket](_hash_get_newblock_from_oldbucket.md)

## Simplified Source
```c
Bucket _hash_get_newbucket_from_oldbucket(Relation rel, Bucket old_bucket,
                                         uint32 lowmask, uint32 maxbucket) {
    // Calculate new bucket by OR'ing with MSB of current table half
    Bucket new_bucket = CALC_NEW_BUCKET(old_bucket, lowmask);

    // If new bucket exceeds table size, try with shifted mask
    if (new_bucket > maxbucket) {
        lowmask = lowmask >> 1;
        new_bucket = CALC_NEW_BUCKET(old_bucket, lowmask);
    }

    return new_bucket;
}
```

## Notes and Other Information
The function uses the CALC_NEW_BUCKET macro which performs the core operation of OR'ing the old bucket with the most significant bit of the lowmask. The two-step process (trying current lowmask, then shifting right if needed) ensures that the function always returns a valid new bucket number within the current table boundaries. This is crucial for maintaining consistency during hash table expansion operations.