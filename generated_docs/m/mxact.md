# mxact

## Location
[src/backend/access/transam/multixact.c:3509-3566](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/multixact.c#L3509-L3566)

## Overview
mxact is a typedef struct used within the pg_get_multixact_members function to store state information for iterating through multixact members when returning set-returning function results.

## Definition


## Detailed Description
This struct serves as a context holder for the pg_get_multixact_members set-returning function (SRF). It stores the array of multixact members retrieved for a given MultiXactId, the total count of members, and an iterator index for tracking progress through the member list during successive function calls. The struct is allocated in the SRF's multi-call memory context to persist across multiple function invocations.

## Parameters / Member Variables
- : Pointer to an array of MultiXactMember structures containing the actual member data
- : Total number of members in the multixact
- : Current iteration index for tracking position when returning members one by one

## Dependencies
- Functions called/Symbols referenced:
  - Used within pg_get_multixact_members function
  - References MultiXactMember type
  - Used with SRF (Set Returning Function) infrastructure
- Called from:
  - Referenced internally within pg_get_multixact_members function

## Notes and Other Information
- This is a local typedef struct defined within pg_get_multixact_members function scope
- Used specifically for SRF state management to return multixact member information
- Memory is allocated in the function's multi-call memory context for persistence
- Part of the SQL-callable function interface for inspecting multixact contents
- Located in src/backend/access/transam/multixact.c:3509-3566