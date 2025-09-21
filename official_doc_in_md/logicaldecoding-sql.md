47.4. Logical Decoding SQL Interface  
---  
[Prev](logicaldecoding-walsender.md "47.3. Streaming Replication Protocol Interface") | [Up](logicaldecoding.md "Chapter 47. Logical Decoding")| Chapter 47. Logical Decoding| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](logicaldecoding-catalogs.md "47.5. System Catalogs Related to Logical Decoding")  
  
* * *

## 47.4. Logical Decoding SQL Interface #

See [Section 9.28.6](functions-admin.md#FUNCTIONS-REPLICATION "9.28.6. Replication Management Functions") for detailed documentation on the SQL-level API for interacting with logical decoding. 

Synchronous replication (see [Section 26.2.8](warm-standby.md#SYNCHRONOUS-REPLICATION "26.2.8. Synchronous Replication")) is only supported on replication slots used over the streaming replication interface. The function interface and additional, non-core interfaces do not support synchronous replication. 

* * *

[Prev](logicaldecoding-walsender.md "47.3. Streaming Replication Protocol Interface") | [Up](logicaldecoding.md "Chapter 47. Logical Decoding")|  [Next](logicaldecoding-catalogs.md "47.5. System Catalogs Related to Logical Decoding")  
---|---|---  
47.3. Streaming Replication Protocol Interface | [Home](index.md "PostgreSQL 17.5 Documentation")|  47.5. System Catalogs Related to Logical Decoding
