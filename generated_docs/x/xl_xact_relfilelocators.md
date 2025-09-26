# xl_xact_relfilelocators

## Location
[src/include/access/xact.h:268-272](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/xact.h#L268-L272)

## Overview
A structure used in PostgreSQL's Write-Ahead Logging (WAL) to record information about relation file locators that are being committed or aborted during transaction processing.

## Definition

```c
typedef struct xl_xact_relfilelocators
{
	int			nrels;			/* number of relations */
	RelFileLocator xlocators[FLEXIBLE_ARRAY_MEMBER];
} xl_xact_relfilelocators;
```
## Detailed Description
The xl_xact_relfilelocators structure is a WAL record format used to log relation file locator information during transaction commit and abort operations. This structure is part of PostgreSQL's transaction logging mechanism and helps track which relation files are affected by a transaction. The structure uses a flexible array member to accommodate a variable number of RelFileLocator entries, making it efficient for transactions that affect different numbers of relations.

## Parameters / Member Variables
- : An integer specifying the number of relations whose file locators are stored in this record
- : A flexible array of RelFileLocator structures containing the actual file locator information for each affected relation

## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER
  - RelFileLocator (implicitly referenced)

- Called from (representative examples):
  - ParseCommitRecord (in xactdesc.c:78)
  - ParseAbortRecord (in xactdesc.c:184)  
  - XactLogCommitRecord (in xact.c:5765)
  - XactLogAbortRecord (in xact.c:5934)
  - MinSizeOfXactRelfileLocators (in xact.h:273)

## Notes and Other Information
- This structure is primarily used during WAL logging for transaction commit and abort records
- The flexible array member design allows for efficient storage of variable numbers of relation file locators
- It's closely tied to PostgreSQL's transaction management and crash recovery mechanisms
- The structure is defined in src/include/access/xact.h at lines 268-272