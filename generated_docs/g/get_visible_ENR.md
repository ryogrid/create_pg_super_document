# get_visible_ENR

## Location
[src/backend/parser/parse_enr.c:26-29](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_enr.c#L26-L29)

## Overview
Retrieves the metadata for a visible Ephemeral Named Relation (ENR) by name from the current parse state context.

## Definition
EphemeralNamedRelationMetadata get_visible_ENR(ParseState *pstate, const char *refname)

## Detailed Description
This function returns the metadata structure for a specified ENR if it exists and is visible within the current query environment. It serves as a direct interface to access ENR metadata, which contains essential information about temporary named relations such as their structure, column definitions, and other properties. The function is a simple wrapper around get_visible_ENR_metadata that operates within the parsing context.

## Parameters / Member Variables
- pstate: Pointer to the ParseState structure containing the current parsing context and query environment
- refname: String containing the name of the ENR to retrieve metadata for

## Dependencies
- Functions called/Symbols referenced:
  - [get_visible_ENR_metadata](get_visible_ENR_metadata.md)
- Called from (representative examples):
  - [addRangeTableEntryForENR](../a/addRangeTableEntryForENR.md)

## Notes and Other Information
- Returns EphemeralNamedRelationMetadata structure if the ENR is found, NULL otherwise
- Part of PostgreSQL's parser infrastructure for handling temporary named relations
- Used when the parser needs to access detailed information about an ENR beyond just checking its existence
- Located in src/backend/parser/parse_enr.c:26-29
- Commonly used in conjunction with name_matches_visible_ENR for ENR processing

## Simplified Source

```c
EphemeralNamedRelationMetadata
get_visible_ENR(ParseState *pstate, const char *refname)
{
    return get_visible_ENR_metadata(pstate->p_queryEnv, refname);
}
```