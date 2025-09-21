66.4. Two-Phase Transactions  
---  
[Prev](subxacts.md "66.3. Subtransactions") | [Up](transactions.md "Chapter 66. Transaction Processing")| Chapter 66. Transaction Processing| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](bki.md "Chapter 67. System Catalog Declarations and Initial Contents")  
  
* * *

## 66.4. Two-Phase Transactions #

PostgreSQL supports a two-phase commit (2PC) protocol that allows multiple distributed systems to work together in a transactional manner. The commands are `PREPARE TRANSACTION`, `COMMIT PREPARED` and `ROLLBACK PREPARED`. Two-phase transactions are intended for use by external transaction management systems. PostgreSQL follows the features and model proposed by the X/Open XA standard, but does not implement some less often used aspects. 

When the user executes `PREPARE TRANSACTION`, the only possible next commands are `COMMIT PREPARED` or `ROLLBACK PREPARED`. In general, this prepared state is intended to be of very short duration, but external availability issues might mean transactions stay in this state for an extended interval. Short-lived prepared transactions are stored only in shared memory and WAL. Transactions that span checkpoints are recorded in the `pg_twophase` directory. Transactions that are currently prepared can be inspected using [`pg_prepared_xacts`](view-pg-prepared-xacts.md "52.16. pg_prepared_xacts"). 

* * *

[Prev](subxacts.md "66.3. Subtransactions") | [Up](transactions.md "Chapter 66. Transaction Processing")|  [Next](bki.md "Chapter 67. System Catalog Declarations and Initial Contents")  
---|---|---  
66.3. Subtransactions | [Home](index.md "PostgreSQL 17.5 Documentation")|  Chapter 67. System Catalog Declarations and Initial Contents
