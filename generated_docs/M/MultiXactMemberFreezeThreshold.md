# MultiXactMemberFreezeThreshold

## Location
[src/backend/access/transam/multixact.c:2970-3006](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/multixact.c#L2970-L3006)

## Overview
Calculates an effective freeze threshold for multixacts based on member space utilization to prevent exhaustion of the multixact members area.

## Definition

```c
typedef struct mxtruncinfo
{
	int64		earliestExistingPage;
} mxtruncinfo;
```
## Detailed Description
This function implements an adaptive mechanism to prevent exhaustion of the multixact members space by dynamically adjusting the effective freeze threshold. When multixact member space utilization exceeds safe thresholds, it reduces the effective autovacuum_multixact_freeze_max_age to trigger more aggressive vacuuming.

The function works by calculating what fraction of member space is being used beyond the safe threshold, then determining how many multixacts should be considered "victims" for elimination. This creates a target that becomes more aggressive as member space utilization increases. In extreme cases, it can return 0, causing every vacuum operation to freeze every multixact.

The algorithm helps prevent scenarios where tables with many-member multixacts could exhaust member space while still staying within normal multixact count limits.

## Parameters
No parameters - the function reads current system state internally.

## Dependencies
- Functions called/Symbols referenced:
  - [ReadMultiXactCounts](../R/ReadMultiXactCounts.md)
  - MULTIXACT_MEMBER_SAFE_THRESHOLD
  - MULTIXACT_MEMBER_DANGER_THRESHOLD
  - autovacuum_multixact_freeze_max_age
  - Min
- Called from (representative examples):
  - [vacuum_get_cutoffs](../v/vacuum_get_cutoffs.md) (src/backend/commands/vacuum.c:1136)
  - [do_start_worker](../d/do_start_worker.md) (src/backend/postmaster/autovacuum.c:1123)
  - [do_autovacuum](../d/do_autovacuum.md) (src/backend/postmaster/autovacuum.c:1914)
  - SizeOfMultiXactTruncate (src/include/access/multixact.h:146)

## Notes and Other Information
- Returns an effective freeze threshold (0 to autovacuum_multixact_freeze_max_age)
- Returns 0 if member space utilization cannot be determined (assumes worst case)
- Returns normal autovacuum_multixact_freeze_max_age if utilization is below safe threshold
- Implements progressive aggressiveness based on member space utilization fraction
- Critical for preventing multixact member space exhaustion
- Works in conjunction with vacuum_get_cutoffs() to influence freeze behavior
- Function is located at src/backend/access/transam/multixact.c:2970-3006

## Simplified Source

```c
int
MultiXactMemberFreezeThreshold(void)
{
    MultiXactOffset members;
    uint32 multixacts;

    // Can't determine usage - assume worst case
    if (!ReadMultiXactCounts(&multixacts, &members))
        return 0;

    // Member space usage is low - no special action needed
    if (members <= MULTIXACT_MEMBER_SAFE_THRESHOLD)
        return autovacuum_multixact_freeze_max_age;

    // Calculate how aggressively we need to freeze based on member usage
    double fraction = (double)(members - MULTIXACT_MEMBER_SAFE_THRESHOLD) /
                     (MULTIXACT_MEMBER_DANGER_THRESHOLD - MULTIXACT_MEMBER_SAFE_THRESHOLD);

    uint32 victim_multixacts = multixacts * fraction;

    // Calculate effective freeze threshold
    if (victim_multixacts > multixacts)
        return 0;  // Maximum aggression - freeze everything

    int result = multixacts - victim_multixacts;

    // Never be less aggressive than normal autovacuum settings
    return Min(result, autovacuum_multixact_freeze_max_age);
}
```