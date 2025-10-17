# equalPolicy

## Location
[src/backend/utils/cache/relcache.c:953-998](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L953-L998)

## Overview
Determines whether two RowSecurityPolicy structures are equivalent by comparing their command types, sublink flags, names, roles, and qualification expressions.

## Definition

```c
static bool
equalPolicy(RowSecurityPolicy *policy1, RowSecurityPolicy *policy2)
```
## Detailed Description
This function performs a comprehensive comparison of two row security policy structures to determine equivalence. It compares all significant attributes of the policies including the command type they apply to, whether they contain sublinks, the policy names, the roles they apply to, and both the main qualification expression and the with-check qualification expression.

The function handles null pointer cases and performs deep comparison of array data for roles and Node structures for qualification expressions.

## Parameters / Member Variables
- `*policy1`: First RowSecurityPolicy structure to compare (may be NULL)
- `*policy2`: Second RowSecurityPolicy structure to compare (may be NULL)
## Dependencies
- Functions called/Symbols referenced:
  - [RowSecurityPolicy](../R/RowSecurityPolicy.md) (structure type)
  - ARR_DIMS (macro for array dimensions)
  - ARR_DATA_PTR (macro for array data pointer)
  - [equal](equal.md) (function for comparing Node structures)
- Called from (representative examples):
  - [equalRSDesc](equalRSDesc.md)

## Notes and Other Information
- Compares policy command types (polcmd) which determine what operations the policy applies to (SELECT, INSERT, UPDATE, DELETE)
- Checks hassublinks flag which indicates whether the policy expressions contain subqueries
- Performs string comparison on policy names using strcmp
- Compares role arrays by first checking dimensions then comparing individual OIDs
- Uses equal() function for deep comparison of qual and with_check_qual Node expressions
- Part of PostgreSQL's Row Level Security (RLS) system for fine-grained access control

## Simplified Source

```c
static bool equalPolicy(RowSecurityPolicy *policy1, RowSecurityPolicy *policy2) {
    // Handle null cases: both null = equal, one null = not equal
    if (policy1 != NULL) {
        if (policy2 == NULL)
            return false;

        // Compare basic policy attributes
        if (policy1->polcmd != policy2->polcmd ||
            policy1->hassublinks != policy2->hassublinks ||
            strcmp(policy1->policy_name, policy2->policy_name) != 0) {
            return false;
        }

        // Compare role arrays: first check dimensions
        if (ARR_DIMS(policy1->roles)[0] != ARR_DIMS(policy2->roles)[0])
            return false;

        // Compare individual role OIDs
        Oid *r1 = (Oid *) ARR_DATA_PTR(policy1->roles);
        Oid *r2 = (Oid *) ARR_DATA_PTR(policy2->roles);

        for (int i = 0; i < ARR_DIMS(policy1->roles)[0]; i++) {
            if (r1[i] != r2[i])
                return false;
        }

        // Compare qualification expressions using deep Node comparison
        if (!equal(policy1->qual, policy2->qual) ||
            !equal(policy1->with_check_qual, policy2->with_check_qual)) {
            return false;
        }
    } else if (policy2 != NULL) {
        // policy1 is NULL but policy2 is not
        return false;
    }

    return true;  // All policy attributes match or both are NULL
}
```