Chapter 47. Logical Decoding  
---  
[Prev](bgworker.md "Chapter 46. Background Worker Processes") | [Up](server-programming.md "Part V. Server Programming")| Part V. Server Programming| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](logicaldecoding-example.md "47.1. Logical Decoding Examples")  
  
* * *

## Chapter 47. Logical Decoding

**Table of Contents**

[47.1. Logical Decoding Examples](logicaldecoding-example.md)
[47.2. Logical Decoding Concepts](logicaldecoding-explanation.md)
    

[47.2.1. Logical Decoding](logicaldecoding-explanation.md#LOGICALDECODING-EXPLANATION-LOG-DEC)
[47.2.2. Replication Slots](logicaldecoding-explanation.md#LOGICALDECODING-REPLICATION-SLOTS)
[47.2.3. Replication Slot Synchronization](logicaldecoding-explanation.md#LOGICALDECODING-REPLICATION-SLOTS-SYNCHRONIZATION)
[47.2.4. Output Plugins](logicaldecoding-explanation.md#LOGICALDECODING-EXPLANATION-OUTPUT-PLUGINS)
[47.2.5. Exported Snapshots](logicaldecoding-explanation.md#LOGICALDECODING-EXPLANATION-EXPORTED-SNAPSHOTS)
[47.3. Streaming Replication Protocol Interface](logicaldecoding-walsender.md)
[47.4. Logical Decoding SQL Interface](logicaldecoding-sql.md)
[47.5. System Catalogs Related to Logical Decoding](logicaldecoding-catalogs.md)
[47.6. Logical Decoding Output Plugins](logicaldecoding-output-plugin.md)
    

[47.6.1. Initialization Function](logicaldecoding-output-plugin.md#LOGICALDECODING-OUTPUT-INIT)
[47.6.2. Capabilities](logicaldecoding-output-plugin.md#LOGICALDECODING-CAPABILITIES)
[47.6.3. Output Modes](logicaldecoding-output-plugin.md#LOGICALDECODING-OUTPUT-MODE)
[47.6.4. Output Plugin Callbacks](logicaldecoding-output-plugin.md#LOGICALDECODING-OUTPUT-PLUGIN-CALLBACKS)
[47.6.5. Functions for Producing Output](logicaldecoding-output-plugin.md#LOGICALDECODING-OUTPUT-PLUGIN-OUTPUT)
[47.7. Logical Decoding Output Writers](logicaldecoding-writer.md)
[47.8. Synchronous Replication Support for Logical Decoding](logicaldecoding-synchronous.md)
    

[47.8.1. Overview](logicaldecoding-synchronous.md#LOGICALDECODING-SYNCHRONOUS-OVERVIEW)
[47.8.2. Caveats](logicaldecoding-synchronous.md#LOGICALDECODING-SYNCHRONOUS-CAVEATS)
[47.9. Streaming of Large Transactions for Logical Decoding](logicaldecoding-streaming.md)
[47.10. Two-phase Commit Support for Logical Decoding](logicaldecoding-two-phase-commits.md)

PostgreSQL provides infrastructure to stream the modifications performed via SQL to external consumers. This functionality can be used for a variety of purposes, including replication solutions and auditing. 

Changes are sent out in streams identified by logical replication slots. 

The format in which those changes are streamed is determined by the output plugin used. An example plugin is provided in the PostgreSQL distribution. Additional plugins can be written to extend the choice of available formats without modifying any core code. Every output plugin has access to each individual new row produced by `INSERT` and the new row version created by `UPDATE`. Availability of old row versions for `UPDATE` and `DELETE` depends on the configured replica identity (see [`REPLICA IDENTITY`](sql-altertable.md#SQL-ALTERTABLE-REPLICA-IDENTITY)). 

Changes can be consumed either using the streaming replication protocol (see [Section 53.4](protocol-replication.md "53.4. Streaming Replication Protocol") and [Section 47.3](logicaldecoding-walsender.md "47.3. Streaming Replication Protocol Interface")), or by calling functions via SQL (see [Section 47.4](logicaldecoding-sql.md "47.4. Logical Decoding SQL Interface")). It is also possible to write additional methods of consuming the output of a replication slot without modifying core code (see [Section 47.7](logicaldecoding-writer.md "47.7. Logical Decoding Output Writers")). 

* * *

[Prev](bgworker.md "Chapter 46. Background Worker Processes") | [Up](server-programming.md "Part V. Server Programming")|  [Next](logicaldecoding-example.md "47.1. Logical Decoding Examples")  
---|---|---  
Chapter 46. Background Worker Processes | [Home](index.md "PostgreSQL 17.5 Documentation")|  47.1. Logical Decoding Examples
