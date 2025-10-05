# macaddr_abbrev_abort

## Location
[src/backend/utils/adt/mac.c:415-482](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/mac.c#L415-L482)

## Overview
A callback function that evaluates the effectiveness of abbreviated key optimization for MAC address sorting operations and determines whether to abort the abbreviation strategy.

## Definition

```c
static bool
macaddr_abbrev_abort(int memtupcount, SortSupport ssup)
```
## Detailed Description
This function serves as a cardinality estimation callback for PostgreSQL's sort support system when sorting MAC addresses with abbreviated keys. It analyzes the effectiveness of the abbreviation optimization by examining the cardinality (number of distinct values) of abbreviated keys compared to the total input count. The function uses HyperLogLog estimation to determine if the abbreviated key optimization is providing sufficient benefit to justify its overhead.

The function implements a sophisticated heuristic to decide when to abort abbreviation:
- If there are more than 100,000 distinct abbreviated values, abbreviation is considered highly effective and continues
- If cardinality is below the threshold (approximately 1 distinct value per 2000 inputs), abbreviation is aborted as ineffective
- The function only operates after processing at least 10,000 tuples to ensure statistical significance

## Parameters / Member Variables
- `memtupcount`: Number of tuples currently in memory for sorting
- `ssup`: SortSupport structure containing sorting optimization state and callbacks
## Dependencies
- Functions called/Symbols referenced:
  - : Estimates cardinality using HyperLogLog algorithm
  - : Sort support structure type
  - : MAC address-specific sort support state
  - : Conditional compilation macro for sort tracing
  - : Platform-specific format string for 64-bit integers
- Called from (representative examples):
  - : Sets this function as the abbreviation abort callback

## Notes and Other Information
- This is a static function internal to the MAC address data type implementation
- The function pays no attention to non-abbreviated data cardinality since MAC address comparison has no equality fast-path
- Uses a target minimum cardinality of 1 per ~2000 non-null inputs with a 0.5 fudge factor
- Includes extensive debug logging when TRACE_SORT is enabled
- The 100k distinct value threshold represents a point where abbreviation benefits outweigh costs even for very large datasets

## Simplified Source

```c
static bool
macaddr_abbrev_abort(int memtupcount, SortSupport ssup)
{
    macaddr_sortsupport_state *uss = ssup->ssup_extra;

    // Only evaluate after processing sufficient data
    if (memtupcount < 10000 || uss->input_count < 10000 || !uss->estimating)
        return false;

    // Estimate cardinality using HyperLogLog
    double abbr_card = estimateHyperLogLog(&uss->abbr_card);

    // High cardinality: abbreviation is very effective, keep going
    if (abbr_card > 100000.0) {
        uss->estimating = false;  // Stop counting, we're committed
        return false;
    }

    // Low cardinality: abort if below threshold (1 per ~2k inputs)
    if (abbr_card < uss->input_count / 2000.0 + 0.5) {
        return true;  // Abort abbreviation
    }

    return false;  // Continue abbreviation
}
```