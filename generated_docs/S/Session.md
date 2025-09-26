# Session

## Location
[src/test/isolation/isolationtester.h:31-32](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/isolation/isolationtester.h#L31-L32)

## Overview
A struct that encapsulates elements of a user's session, primarily managing state for parallel query execution and other session-scoped resources.

## Definition

```c
struct Step
{
	char	   *name;
	char	   *sql;
	/* These fields are filled by check_testspec(): */
	int			session;		/* identifies owning session */
	bool		used;			/* has step been used in a permutation? */
};
```
## Detailed Description
The Session struct is designed to manage session-scoped state that was previously handled by global variables. It serves as a container for resources that need to be shared across parallel workers within a single user session. The structure primarily focuses on parallel query execution infrastructure, providing shared memory management through DSM (Dynamic Shared Memory) segments and DSA (Dynamic Shared Areas). It also manages type cache state that needs to be shared between parallel processes.

## Parameters / Member Variables
- : Pointer to the session-scoped DSM (Dynamic Shared Memory) segment used for parallel execution
- : Pointer to the session-scoped DSA (Dynamic Shared Area) for memory allocation within the shared segment
- : Registry for shared record type modifier information managed by typcache.c
- : Hash table for shared record type information
- : Hash table for shared type modifier information

## Dependencies
- Functions called/Symbols referenced:
  - [dsm_segment](../d/dsm_segment.md)
  - [dsa_area](../d/dsa_area.md)
  - [SharedRecordTypmodRegistry](SharedRecordTypmodRegistry.md)
  - [dshash_table](../d/dshash_table.md)
- Called from (representative examples):
  - [InitializeSession](../I/InitializeSession.md)
  - [check_testspec](../c/check_testspec.md)
  - [Permutation](../P/Permutation.md)

## Notes and Other Information
- This struct is part of PostgreSQL's parallel query infrastructure
- It consolidates session state that was previously managed through global variables
- The design allows for better isolation and management of session-specific resources
- Located in src/include/access/session.h, indicating its role in the access layer
- The structure is extensible and could include additional session-scoped state in the future