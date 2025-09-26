# xl_xact_dbinfo

## Location
[src/include/access/xact.h:255-259](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/xact.h#L255-L259)

## Overview
WAL record sub-structure that stores database and tablespace information for commit and abort transaction records when database context is needed.

## Definition
```c
typedef struct xl_xact_dbinfo
{
    Oid     dbId;       /* MyDatabaseId */
    Oid     tsId;       /* MyDatabaseTableSpace */
} xl_xact_dbinfo;
```

## Detailed Description
xl_xact_dbinfo is a sub-record structure used within commit and abort WAL records to store database context information. This information is included when the transaction record needs to convey which database and tablespace the transaction was operating in, which is essential for certain recovery and logical decoding operations.

The structure is included in WAL records when the XACT_XINFO_HAS_DBINFO flag is set in the xl_xact_xinfo structure. This typically occurs when:
1. The transaction has relcache invalidations that require database context
2. Logical decoding is active and needs database information for proper processing
3. There are shared invalidation messages that need database context

The database ID (dbId) corresponds to MyDatabaseId, which identifies the specific database where the transaction executed. The tablespace ID (tsId) corresponds to MyDatabaseTableSpace, which identifies the default tablespace for the database. This information allows standby servers and logical decoding processes to properly interpret and apply transaction changes in the correct database context.

## Parameters / Member Variables
- `dbId`: Object ID of the database where the transaction executed (corresponds to MyDatabaseId)
- `tsId`: Object ID of the tablespace for the database (corresponds to MyDatabaseTableSpace)

## Dependencies
- Functions called/Symbols referenced:
  - Oid (standard PostgreSQL object ID type)
- Called from (representative examples):
  - ParseCommitRecord (extracts database info from commit records)
  - ParseAbortRecord (extracts database info from abort records)
  - XactLogCommitRecord (includes database info in commit records)
  - XactLogAbortRecord (includes database info in abort records)

## Notes and Other Information
- Located in src/include/access/xact.h:255-259
- Only included in WAL records when XACT_XINFO_HAS_DBINFO flag is set
- Essential for relcache invalidation processing on standby servers
- Required for logical decoding to maintain proper database context
- Used during WAL replay to ensure operations are applied in the correct database context
- The values correspond to global variables MyDatabaseId and MyDatabaseTableSpace from the originating transaction
- Critical for multi-database PostgreSQL installations where transactions in different databases need to be distinguished during recovery