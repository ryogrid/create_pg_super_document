# should_attempt_truncation

## Location
[src/backend/access/heap/vacuumlazy.c:2530-2549](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/vacuumlazy.c#L2530-L2549)

## Overview
Determines whether vacuum should attempt to truncate the heap relation by evaluating cost-benefit factors and system constraints.

## Definition
```c
static bool
should_attempt_truncation(LVRelState *vacrel)
```

## Detailed Description
This function evaluates whether heap truncation should be attempted during vacuum operations. It implements several safety checks and efficiency heuristics to determine if truncation is worthwhile. The function first checks if truncation is disabled or if the vacuum failsafe is active, which would prevent safe truncation. It then calculates the number of potentially freeable pages and applies minimum thresholds to ensure truncation will provide meaningful space savings. The decision considers both absolute page counts (REL_TRUNCATE_MINIMUM) and relative thresholds (REL_TRUNCATE_FRACTION) to balance truncation benefits against the cost of acquiring AccessExclusiveLock.

## Parameters / Member Variables
- `vacrel`: Vacuum relation state containing page counts and truncation settings

## Dependencies
- Functions called/Symbols referenced:
  - [LVRelState](../L/LVRelState.md)
  - VacuumFailsafeActive
  - REL_TRUNCATE_MINIMUM
  - REL_TRUNCATE_FRACTION
  - BlockNumber
- Called from (representative examples):
  - [heap_vacuum_rel](../h/heap_vacuum_rel.md)

## Notes and Other Information
The function includes important safeguards against truncation during XID wraparound failsafe conditions, where acquiring AccessExclusiveLock could worsen system-wide XID exhaustion problems. Truncation requires AccessExclusiveLock which can be particularly disruptive on hot standby systems, so the function ensures meaningful space savings justify the cost. The thresholds ensure truncation attempts only when there's a reasonable chance of releasing a significant number of pages.

## Simplified Source

```c
static bool
should_attempt_truncation(LVRelState *vacrel)
{
    BlockNumber possibly_freeable;

    // Don't truncate if disabled or failsafe is active
    if (!vacrel->do_rel_truncate || VacuumFailsafeActive)
        return false;

    // Calculate potentially freeable pages
    possibly_freeable = vacrel->rel_pages - vacrel->nonempty_pages;

    // Check if truncation meets minimum thresholds
    if (possibly_freeable > 0 &&
        (possibly_freeable >= REL_TRUNCATE_MINIMUM ||
         possibly_freeable >= vacrel->rel_pages / REL_TRUNCATE_FRACTION))
        return true;

    return false;
}
```