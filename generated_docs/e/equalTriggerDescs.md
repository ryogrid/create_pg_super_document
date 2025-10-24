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

## Simplified Source

```c
bool equalTriggerDescs(TriggerDesc *trigdesc1, TriggerDesc *trigdesc2) {
    // Handle NULL cases
    if (trigdesc1 == NULL && trigdesc2 == NULL)
        return true;
    if (trigdesc1 == NULL || trigdesc2 == NULL)
        return false;

    // Check trigger count
    if (trigdesc1->numtriggers != trigdesc2->numtriggers)
        return false;

    // Compare each trigger in order (ordering is significant)
    for (int i = 0; i < trigdesc1->numtriggers; i++) {
        Trigger *trig1 = trigdesc1->triggers + i;
        Trigger *trig2 = trigdesc2->triggers + i;

        // Compare basic properties
        if (trig1->tgoid != trig2->tgoid ||
            strcmp(trig1->tgname, trig2->tgname) != 0 ||
            trig1->tgfoid != trig2->tgfoid ||
            trig1->tgtype != trig2->tgtype ||
            trig1->tgenabled != trig2->tgenabled)
            return false;

        // Compare internal and constraint properties
        if (trig1->tgisinternal != trig2->tgisinternal ||
            trig1->tgisclone != trig2->tgisclone ||
            trig1->tgconstrrelid != trig2->tgconstrrelid ||
            trig1->tgconstrindid != trig2->tgconstrindid ||
            trig1->tgconstraint != trig2->tgconstraint ||
            trig1->tgdeferrable != trig2->tgdeferrable ||
            trig1->tginitdeferred != trig2->tginitdeferred)
            return false;

        // Compare argument properties
        if (trig1->tgnargs != trig2->tgnargs ||
            trig1->tgnattr != trig2->tgnattr)
            return false;

        // Compare attribute array if present
        if (trig1->tgnattr > 0 &&
            memcmp(trig1->tgattr, trig2->tgattr, trig1->tgnattr * sizeof(int16)) != 0)
            return false;

        // Compare trigger arguments
        for (int j = 0; j < trig1->tgnargs; j++) {
            if (strcmp(trig1->tgargs[j], trig2->tgargs[j]) != 0)
                return false;
        }

        // Compare optional string fields (NULL-safe)
        if (!strings_equal_null_safe(trig1->tgqual, trig2->tgqual) ||
            !strings_equal_null_safe(trig1->tgoldtable, trig2->tgoldtable) ||
            !strings_equal_null_safe(trig1->tgnewtable, trig2->tgnewtable))
            return false;
    }

    return true;
}

// Helper for NULL-safe string comparison
static bool strings_equal_null_safe(const char *str1, const char *str2) {
    if (str1 == NULL && str2 == NULL) return true;
    if (str1 == NULL || str2 == NULL) return false;
    return strcmp(str1, str2) == 0;
}
```