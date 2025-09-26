# equalTriggerDescs

## Location
[src/backend/commands/trigger.c:2177-2271](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L2177-L2271)

## Overview
equalTriggerDescs compares two TriggerDesc structures for exact equality, performing deep comparison of all trigger properties to determine if they represent identical trigger configurations.

## Definition
bool equalTriggerDescs(TriggerDesc *trigdesc1, TriggerDesc *trigdesc2)

## Detailed Description
This function performs comprehensive equality testing between two TriggerDesc structures by comparing all relevant fields. The comparison strategy includes:

1. **Null Handling**: Handles all combinations of NULL pointers - returns true only if both are NULL, false if only one is NULL.

2. **Structural Comparison**: First compares the number of triggers in each descriptor - if different, the descriptors are not equal.

3. **Positional Comparison**: Since PostgreSQL 7.3, trigger ordering is considered significant, so triggers are compared in their array positions rather than searching for matches.

4. **Comprehensive Field Comparison**: For each trigger pair, compares all fields:
   - **Basic Properties**: tgoid, tgname, tgfoid, tgtype, tgenabled
   - **Internal Properties**: tgisinternal, tgisclone
   - **Constraint Properties**: tgconstrrelid, tgconstrindid, tgconstraint, tgdeferrable, tginitdeferred
   - **Argument Properties**: tgnargs, tgnattr
   - **Array Fields**: tgattr (using memcmp for attribute arrays), tgargs (string-by-string comparison)
   - **Optional String Fields**: tgqual, tgoldtable, tgnewtable (with NULL-safe comparison)

5. **String Comparison**: Uses strcmp() for string comparisons and handles NULL pointers explicitly for optional fields.

6. **Binary Comparison**: Uses memcmp() for the tgattr array comparison when attribute count is greater than zero.

The function is designed for exact equality detection, such as checking cache entry staleness, and is sensitive to parse column locations in WHEN clauses.

## Parameters / Member Variables
- : First TriggerDesc structure to compare (can be NULL)
- : Second TriggerDesc structure to compare (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - strcmp: String comparison for trigger names and other string fields
  - memcmp: Binary comparison for attribute arrays

- Called from (representative examples):
  - Currently not referenced by other functions in the analyzed codebase

## Notes and Other Information
- Returns true if both TriggerDesc structures are identical in all respects
- [Trigger](../T/Trigger.md) order is significant - triggers must match in the same array positions
- Sensitive to parse column locations in WHEN clause comparisons
- Primarily used for cache validation and staleness detection
- Handles NULL pointers safely for all optional string fields
- Does not compare hint flags as they should be derivable from trigger properties
- The comparison is very strict - any difference in any field results in false
- Useful for determining if a cached trigger descriptor needs to be rebuilt