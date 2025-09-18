# InitializeSystemUser

## Location
[src/backend/utils/init/miscinit.c:920-943](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/init/miscinit.c#L920-L943)

## Overview
Initializes the system user identifier by combining authentication method and authentication ID into a formatted string for session tracking.

## Definition


## Detailed Description
This function creates and stores the system user identifier by formatting the authentication method and authentication ID into a single string using the pattern "auth_method:authn_id". The resulting string is stored in the TopMemoryContext for session-long persistence. This identifier is used for auditing and tracking purposes to maintain information about how a user was authenticated beyond just their role identity.

## Parameters / Member Variables
- : The authentication identifier (cannot be NULL)
- : The authentication method used (must be valid when authn_id is provided)

## Dependencies
- Functions called/Symbols referenced:
  - [psprintf](../p/psprintf.md) (PostgreSQL's sprintf equivalent)
  - [MemoryContextStrdup](../M/MemoryContextStrdup.md) (memory context string duplication)
  - TopMemoryContext (top-level memory context)
  - [pfree](../p/pfree.md) (PostgreSQL's memory free function)
- Called from (representative examples):
  - [ParallelWorkerMain](../P/ParallelWorkerMain.md) (src/backend/access/transam/parallel.c:1534)
  - [InitPostgres](InitPostgres.md) (src/backend/utils/init/postinit.c:930)
  - AmSpecialWorkerProcess (src/include/miscadmin.h:420)

## Notes and Other Information
- Can only be called once per session (asserts SystemUser is NULL)
- Requires authn_id to be non-NULL (authentication ID must be provided)
- Creates a formatted string in the pattern "auth_method:authn_id"
- Stores the result in TopMemoryContext for session-long persistence
- Part of PostgreSQL's enhanced authentication tracking and auditing system
- Used to maintain authentication context beyond basic role-based identity
- SystemUser global variable holds the result for later access