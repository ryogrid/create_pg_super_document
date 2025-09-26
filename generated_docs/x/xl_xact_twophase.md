# xl_xact_twophase

## Location
[src/include/access/xact.h:303-306](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/xact.h#L303-L306)

## Overview
A simple structure used in PostgreSQL's WAL to record transaction identifiers associated with two-phase commit operations during transaction logging.

## Definition

```c
typedef struct xl_xact_twophase
{
	TransactionId xid;
} xl_xact_twophase;
```
## Detailed Description
The xl_xact_twophase structure is a minimal WAL record format used specifically for logging two-phase commit transaction identifiers. In PostgreSQL's distributed transaction system, two-phase commit (2PC) is a protocol that ensures atomicity across multiple databases or transaction managers. This structure captures the essential information needed to identify a transaction participating in a two-phase commit protocol within WAL records, enabling proper recovery and coordination of distributed transactions during commit and abort operations.

## Parameters / Member Variables
- : A TransactionId representing the identifier of the transaction participating in the two-phase commit protocol

## Dependencies
- Functions called/Symbols referenced:
  - TransactionId (data type)

- Called from (representative examples):
  - ParseCommitRecord (in xactdesc.c:111, 115)
  - ParseAbortRecord (in xactdesc.c:206, 210)
  - XactLogCommitRecord (in xact.c:5768, 5903)
  - XactLogAbortRecord (in xact.c:5936, 6049)

## Notes and Other Information
- Essential component of PostgreSQL's two-phase commit protocol implementation
- Used in both transaction commit and abort scenarios for distributed transactions
- Provides the minimal information needed to identify 2PC transactions in WAL records
- Critical for maintaining consistency in distributed database environments
- Integrates with PostgreSQL's prepared transaction management system
- The structure is defined in src/include/access/xact.h at lines 303-306