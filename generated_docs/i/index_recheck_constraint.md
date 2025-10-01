# index_recheck_constraint

## Location
[src/backend/executor/execIndexing.c:932-962](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execIndexing.c#L932-L962)

## Overview
A static function that validates whether an existing tuple's indexed values truly conflict with new values according to exclusion constraint operators, returning true if a conflict exists.

## Definition

```c
static bool
index_recheck_constraint(Relation index, const Oid *constr_procs,
						 const Datum *existing_values, const bool *existing_isnull,
						 const Datum *new_values)
```
## Detailed Description
This function performs a detailed comparison between an existing tuple's indexed values and new values to determine if they genuinely violate an exclusion constraint. It iterates through all key attributes of the index, applying the appropriate exclusion operators to compare corresponding values. The function assumes that exclusion operators are strict (return NULL when any operand is NULL), and treats NULL values as non-conflicting.

For each key attribute, the function calls the exclusion operator using the index's collation settings. If any operator returns false, indicating no conflict for that attribute, the entire constraint check returns false. Only when all operators return true does the function conclude that a genuine conflict exists.

## Parameters / Member Variables
- : The index relation containing the exclusion constraint definition
- : Array of OIDs representing the exclusion operators for each key attribute
- : Array of Datum values from the existing tuple being compared
- : Array of boolean flags indicating which existing values are NULL
- : Array of Datum values from the new tuple being checked

## Dependencies
- Functions called/Symbols referenced:
  - IndexRelationGetNumberOfKeyAttributes
  - [OidFunctionCall2Coll](../O/OidFunctionCall2Coll.md)
- Called from (representative examples):
  - [check_exclusion_or_unique_constraint](../c/check_exclusion_or_unique_constraint.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the execIndexing.c file
- The function assumes exclusion operators are strict, meaning they return NULL (treated as false) when any operand is NULL
- Uses the index's collation settings when calling comparison operators via OidFunctionCall2Coll
- Part of PostgreSQL's exclusion constraint enforcement mechanism, specifically handling the detailed value comparison logic
- The function performs short-circuit evaluation - if any attribute comparison returns false, the entire constraint check terminates early

## Simplified Source

```c
static bool
index_recheck_constraint(Relation index, const Oid *constr_procs,
                        const Datum *existing_values, const bool *existing_isnull,
                        const Datum *new_values)
{
    int indnkeyatts = IndexRelationGetNumberOfKeyAttributes(index);

    // Check each key attribute for exclusion constraint violations
    for (int i = 0; i < indnkeyatts; i++) {
        // Exclusion operators are strict - NULL values don't conflict
        if (existing_isnull[i])
            return false;

        // Call the exclusion operator to compare values
        bool conflicts = DatumGetBool(OidFunctionCall2Coll(constr_procs[i],
                                                           index->rd_indcollation[i],
                                                           existing_values[i],
                                                           new_values[i]));

        // If any attribute doesn't conflict, there's no overall conflict
        if (!conflicts)
            return false;
    }

    // All attributes conflict - return true
    return true;
}
```