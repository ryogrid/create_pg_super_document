66.2. Transactions and Locking  
---  
[Prev](transaction-id.md "66.1. Transactions and Identifiers") | [Up](transactions.md "Chapter 66. Transaction Processing")| Chapter 66. Transaction Processing| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](subxacts.md "66.3. Subtransactions")  
  
* * *

## 66.2. Transactions and Locking #

The transaction IDs of currently executing transactions are shown in [`pg_locks`](view-pg-locks.md "52.12. pg_locks") in columns `virtualxid` and `transactionid`. Read-only transactions will have `virtualxid`s but NULL `transactionid`s, while both columns will be set in read-write transactions. 

Some lock types wait on `virtualxid`, while other types wait on `transactionid`. Row-level read and write locks are recorded directly in the locked rows and can be inspected using the [pgrowlocks](pgrowlocks.md "F.29. pgrowlocks — show a table's row locking information") extension. Row-level read locks might also require the assignment of multixact IDs (`mxid`; see [Section 24.1.5.1](routine-vacuuming.md#VACUUM-FOR-MULTIXACT-WRAPAROUND "24.1.5.1. Multixacts and Wraparound")). 

* * *

[Prev](transaction-id.md "66.1. Transactions and Identifiers") | [Up](transactions.md "Chapter 66. Transaction Processing")|  [Next](subxacts.md "66.3. Subtransactions")  
---|---|---  
66.1. Transactions and Identifiers | [Home](index.md "PostgreSQL 17.5 Documentation")|  66.3. Subtransactions
