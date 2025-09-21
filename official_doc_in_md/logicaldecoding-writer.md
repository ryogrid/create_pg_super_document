47.7. Logical Decoding Output Writers  
---  
[Prev](logicaldecoding-output-plugin.md "47.6. Logical Decoding Output Plugins") | [Up](logicaldecoding.md "Chapter 47. Logical Decoding")| Chapter 47. Logical Decoding| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](logicaldecoding-synchronous.md "47.8. Synchronous Replication Support for Logical Decoding")  
  
* * *

## 47.7. Logical Decoding Output Writers #

It is possible to add more output methods for logical decoding. For details, see `src/backend/replication/logical/logicalfuncs.c`. Essentially, three functions need to be provided: one to read WAL, one to prepare writing output, and one to write the output (see [Section 47.6.5](logicaldecoding-output-plugin.md#LOGICALDECODING-OUTPUT-PLUGIN-OUTPUT "47.6.5. Functions for Producing Output")). 

* * *

[Prev](logicaldecoding-output-plugin.md "47.6. Logical Decoding Output Plugins") | [Up](logicaldecoding.md "Chapter 47. Logical Decoding")|  [Next](logicaldecoding-synchronous.md "47.8. Synchronous Replication Support for Logical Decoding")  
---|---|---  
47.6. Logical Decoding Output Plugins | [Home](index.md "PostgreSQL 17.5 Documentation")|  47.8. Synchronous Replication Support for Logical Decoding
