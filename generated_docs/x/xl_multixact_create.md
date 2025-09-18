# xl_multixact_create

## Location
[src/include/access/multixact.h:73-79](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/multixact.h#L73-L79)

## Overview
xl_multixact_create is a WAL (Write-Ahead Log) record structure used to log the creation of new multi-transaction IDs in PostgreSQL's transaction system.

## Definition
typedef struct xl_multixact_create
{
    MultiXactId mid;            /* new MultiXact's ID */
    MultiXactOffset moff;       /* its starting offset in members file */
    int32       nmembers;       /* number of member XIDs */
    MultiXactMember members[FLEXIBLE_ARRAY_MEMBER];
} xl_multixact_create;

## Detailed Description
xl_multixact_create is a critical WAL record structure that captures the information needed to recreate a multi-transaction during recovery or replication. When a new multi-transaction is created (typically when multiple transactions need to hold different types of locks on the same tuple), this structure is written to the WAL to ensure durability and recoverability.

The structure contains all the essential information about the multi-transaction: its unique identifier, where its member information is stored in the members file, how many transactions are part of it, and the actual details of each participating transaction. This allows the system to fully reconstruct the multi-transaction state during crash recovery or on replica servers.

## Parameters / Member Variables
- : The newly assigned MultiXactId that uniquely identifies this multi-transaction
- : The MultiXactOffset indicating the starting position in the pg_multixact/members file where this multi-transaction's member data is stored
- : The number of individual transactions (MultiXactMembers) that are part of this multi-transaction
- : A flexible array containing the actual MultiXactMember structures, each representing a transaction ID and its associated lock status

## Dependencies
- Functions called/Symbols referenced:
  - MultiXactId: Type for multi-transaction identifiers
  - MultiXactOffset: Type for offsets into the members file
  - [MultiXactMember](../M/MultiXactMember.md): Structure representing individual transaction members
  - FLEXIBLE_ARRAY_MEMBER: Macro for variable-length array members
- Called from (representative examples):
  - [MultiXactIdCreateFromMembers](../M/MultiXactIdCreateFromMembers.md): Creates WAL records using this structure when new multi-transactions are formed
  - [multixact_redo](../m/multixact_redo.md): Processes xl_multixact_create records during recovery to recreate multi-transactions
  - [multixact_desc](../m/multixact_desc.md): Uses this structure for debugging and logging purposes in WAL record descriptions

## Notes and Other Information
- This is specifically a WAL record structure, designed for durability and recovery rather than runtime operations
- The flexible array member allows the structure to accommodate multi-transactions with varying numbers of member transactions
- Used in conjunction with XLOG_MULTIXACT_CREATE_ID WAL record type
- Essential for maintaining consistency across crashes, restarts, and in streaming replication scenarios
- The SizeOfMultiXactCreate macro is defined to calculate the base size excluding the flexible array portion
- Part of PostgreSQL's robust crash recovery system ensuring that complex locking states can be properly restored