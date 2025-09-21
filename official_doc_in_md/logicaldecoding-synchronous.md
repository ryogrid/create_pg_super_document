47.8. Synchronous Replication Support for Logical Decoding  
---  
[Prev](logicaldecoding-writer.md "47.7. Logical Decoding Output Writers") | [Up](logicaldecoding.md "Chapter 47. Logical Decoding")| Chapter 47. Logical Decoding| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](logicaldecoding-streaming.md "47.9. Streaming of Large Transactions for Logical Decoding")  
  
* * *

## 47.8. Synchronous Replication Support for Logical Decoding #

[47.8.1. Overview](logicaldecoding-synchronous.md#LOGICALDECODING-SYNCHRONOUS-OVERVIEW)
[47.8.2. Caveats](logicaldecoding-synchronous.md#LOGICALDECODING-SYNCHRONOUS-CAVEATS)

### 47.8.1. Overview #

Logical decoding can be used to build [synchronous replication](warm-standby.md#SYNCHRONOUS-REPLICATION "26.2.8. Synchronous Replication") solutions with the same user interface as synchronous replication for [streaming replication](warm-standby.md#STREAMING-REPLICATION "26.2.5. Streaming Replication"). To do this, the streaming replication interface (see [Section 47.3](logicaldecoding-walsender.md "47.3. Streaming Replication Protocol Interface")) must be used to stream out data. Clients have to send `Standby status update (F)` (see [Section 53.4](protocol-replication.md "53.4. Streaming Replication Protocol")) messages, just like streaming replication clients do. 

### Note

A synchronous replica receiving changes via logical decoding will work in the scope of a single database. Since, in contrast to that, _`synchronous_standby_names`_ currently is server wide, this means this technique will not work properly if more than one database is actively used. 

### 47.8.2. Caveats #

In synchronous replication setup, a deadlock can happen, if the transaction has locked [user] catalog tables exclusively. See [Section 47.6.2](logicaldecoding-output-plugin.md#LOGICALDECODING-CAPABILITIES "47.6.2. Capabilities") for information on user catalog tables. This is because logical decoding of transactions can lock catalog tables to access them. To avoid this users must refrain from taking an exclusive lock on [user] catalog tables. This can happen in the following ways: 

  * Issuing an explicit `LOCK` on `pg_class` in a transaction. 

  * Perform `CLUSTER` on `pg_class` in a transaction. 

  * `PREPARE TRANSACTION` after `LOCK` command on `pg_class` and allow logical decoding of two-phase transactions. 

  * `PREPARE TRANSACTION` after `CLUSTER` command on `pg_trigger` and allow logical decoding of two-phase transactions. This will lead to deadlock only when published table have a trigger. 

  * Executing `TRUNCATE` on [user] catalog table in a transaction. 




Note that these commands can cause deadlocks not only for the system catalog tables listed above but for other catalog tables. 

* * *

[Prev](logicaldecoding-writer.md "47.7. Logical Decoding Output Writers") | [Up](logicaldecoding.md "Chapter 47. Logical Decoding")|  [Next](logicaldecoding-streaming.md "47.9. Streaming of Large Transactions for Logical Decoding")  
---|---|---  
47.7. Logical Decoding Output Writers | [Home](index.md "PostgreSQL 17.5 Documentation")|  47.9. Streaming of Large Transactions for Logical Decoding
