# xl_xact_subxacts

## Location
[src/include/access/xact.h:261-265](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/xact.h#L261-L265)

## Overview
WAL record sub-structure that stores information about subtransactions that were part of a commit or abort transaction record.

## Definition
```c
typedef struct xl_xact_subxacts
{
    int           nsubxacts;    /* number of subtransaction XIDs */
    TransactionId subxacts[FLEXIBLE_ARRAY_MEMBER];
} xl_xact_subxacts;
```

## Detailed Description
xl_xact_subxacts is a sub-record structure used within commit and abort WAL records to record information about subtransactions that were part of the main transaction being committed or aborted. This information is crucial for maintaining transaction hierarchy consistency during WAL replay and recovery operations.

The structure is included in WAL records when the XACT_XINFO_HAS_SUBXACTS flag is set in the xl_xact_xinfo structure, which occurs when the transaction has one or more subtransactions that need to be recorded in the WAL. During commit, all subtransactions are committed along with the parent transaction, and during abort, all subtransactions are likewise aborted.

The structure uses a flexible array member to efficiently store a variable number of subtransaction IDs without requiring fixed-size allocations. This design allows the WAL record to be as compact as possible while still containing all necessary subtransaction information.

During recovery, this information is used to properly reconstruct the transaction hierarchy and ensure that all subtransactions are handled consistently with their parent transaction. This is essential for maintaining ACID properties and ensuring that the recovery process accurately reflects the original transaction's behavior.

## Parameters / Member Variables
- `nsubxacts`: The number of subtransaction XIDs stored in the subxacts array
- `subxacts`: A flexible array containing the transaction IDs of all subtransactions that were part of the parent transaction

## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER
  - TransactionId (type)
- Called from (representative examples):
  - [ParseCommitRecord](../P/ParseCommitRecord.md) (extracts subtransaction info from commit records)
  - [ParseAbortRecord](../P/ParseAbortRecord.md) (extracts subtransaction info from abort records)
  - [XactLogCommitRecord](../X/XactLogCommitRecord.md) (includes subtransaction info in commit records)
  - [XactLogAbortRecord](../X/XactLogAbortRecord.md) (includes subtransaction info in abort records)
  - MinSizeOfXactSubxacts (macro for calculating minimum size)

## Notes and Other Information
- Located in src/include/access/xact.h:261-265
- Only included in WAL records when XACT_XINFO_HAS_SUBXACTS flag is set
- Uses flexible array member for efficient variable-length storage
- MinSizeOfXactSubxacts macro calculates the base size excluding the flexible array
- Essential for maintaining transaction hierarchy during recovery operations
- Ensures that subtransactions are properly committed or aborted along with their parent
- Critical component of PostgreSQL's nested transaction support in the WAL system
- Used during both normal WAL replay and crash recovery scenarios