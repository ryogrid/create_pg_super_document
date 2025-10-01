# get_visible_ENR_metadata

## Location
[src/backend/utils/misc/queryenvironment.c:45-68](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/queryenvironment.c#L45-L68)

## Overview
Retrieves the metadata for a visible ephemeral named relation (ENR) by name from a query environment.

## Definition
```c
EphemeralNamedRelationMetadata get_visible_ENR_metadata(QueryEnvironment *queryEnv, const char *refname)
```

## Detailed Description
This function searches for an ephemeral named relation with the specified name in the given query environment and returns its metadata if found. The function acts as a safe accessor that handles null query environments gracefully and returns null if the ENR doesn't exist. It uses the get_ENR function internally to locate the relation and then extracts the metadata component from it.

## Parameters / Member Variables
- `queryEnv`: The QueryEnvironment structure to search in (can be NULL)
- `refname`: The name of the ephemeral named relation to look up (must not be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [get_ENR](get_ENR.md) (to locate the ENR by name)
  - Assert (for parameter validation)
- Called from (representative examples):
  - [name_matches_visible_ENR](../n/name_matches_visible_ENR.md)
  - [get_visible_ENR](get_visible_ENR.md)

## Notes and Other Information
- Returns NULL if queryEnv is NULL or if the named ENR is not found
- The function includes an assertion that refname must not be NULL
- Returns a pointer to the metadata structure within the ENR, not a copy
- This is a read-only operation that doesn't modify the query environment

## Simplified Source

```c
EphemeralNamedRelationMetadata
get_visible_ENR_metadata(QueryEnvironment *queryEnv, const char *refname)
{
    EphemeralNamedRelation enr;

    Assert(refname != NULL);

    // No query environment means no ENRs available
    if (queryEnv == NULL)
        return NULL;

    // Look up the ENR by name
    enr = get_ENR(queryEnv, refname);

    // Return metadata if ENR exists, NULL otherwise
    if (enr)
        return &(enr->md);

    return NULL;
}
```