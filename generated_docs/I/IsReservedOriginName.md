# IsReservedOriginName

## Location
[src/backend/replication/logical/origin.c:204-220](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/origin.c#L204-L220)

## Overview
A static helper function that determines whether a given replication origin name is reserved by checking if it matches the special names "none" or "any".

## Definition

```c
static bool
IsReservedOriginName(const char *name)
```
## Detailed Description
This function performs a case-insensitive comparison to determine if the provided origin name matches either of the two reserved replication origin names: "none" (represented by LOGICALREP_ORIGIN_NONE) or "any" (represented by LOGICALREP_ORIGIN_ANY). These reserved names have special meaning in the logical replication system and cannot be used for user-defined replication origins. The function helps enforce naming restrictions during replication origin creation.

## Parameters / Member Variables
- `*name`: A null-terminated string containing the replication origin name to check
## Dependencies
- Functions called/Symbols referenced:
  - [pg_strcasecmp](../p/pg_strcasecmp.md)
  - LOGICALREP_ORIGIN_NONE
  - LOGICALREP_ORIGIN_ANY
  - RepOriginId
- Called from (representative examples):
  - [pg_replication_origin_create](../p/pg_replication_origin_create.md)

## Notes and Other Information
- This is a static function, accessible only within the origin.c file
- Uses case-insensitive comparison via pg_strcasecmp, so "None", "NONE", "none" are all considered reserved
- The reserved names "none" and "any" have special semantic meaning in logical replication contexts
- Returns true if the name is reserved, false otherwise
- Serves as input validation for replication origin creation to prevent conflicts with system-reserved names