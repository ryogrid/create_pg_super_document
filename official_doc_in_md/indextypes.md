Chapter 64. Built-in Index Access Methods  
---  
[Prev](custom-rmgr.md "63.2. Custom WAL Resource Managers") | [Up](internals.md "Part VII. Internals")| Part VII. Internals| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](btree.md "64.1. B-Tree Indexes")  
  
* * *

## Chapter 64. Built-in Index Access Methods

**Table of Contents**

[64.1. B-Tree Indexes](btree.md)
    

[64.1.1. Introduction](btree.md#BTREE-INTRO)
[64.1.2. Behavior of B-Tree Operator Classes](btree.md#BTREE-BEHAVIOR)
[64.1.3. B-Tree Support Functions](btree.md#BTREE-SUPPORT-FUNCS)
[64.1.4. Implementation](btree.md#BTREE-IMPLEMENTATION)
[64.2. GiST Indexes](gist.md)
    

[64.2.1. Introduction](gist.md#GIST-INTRO)
[64.2.2. Built-in Operator Classes](gist.md#GIST-BUILTIN-OPCLASSES)
[64.2.3. Extensibility](gist.md#GIST-EXTENSIBILITY)
[64.2.4. Implementation](gist.md#GIST-IMPLEMENTATION)
[64.2.5. Examples](gist.md#GIST-EXAMPLES)
[64.3. SP-GiST Indexes](spgist.md)
    

[64.3.1. Introduction](spgist.md#SPGIST-INTRO)
[64.3.2. Built-in Operator Classes](spgist.md#SPGIST-BUILTIN-OPCLASSES)
[64.3.3. Extensibility](spgist.md#SPGIST-EXTENSIBILITY)
[64.3.4. Implementation](spgist.md#SPGIST-IMPLEMENTATION)
[64.3.5. Examples](spgist.md#SPGIST-EXAMPLES)
[64.4. GIN Indexes](gin.md)
    

[64.4.1. Introduction](gin.md#GIN-INTRO)
[64.4.2. Built-in Operator Classes](gin.md#GIN-BUILTIN-OPCLASSES)
[64.4.3. Extensibility](gin.md#GIN-EXTENSIBILITY)
[64.4.4. Implementation](gin.md#GIN-IMPLEMENTATION)
[64.4.5. GIN Tips and Tricks](gin.md#GIN-TIPS)
[64.4.6. Limitations](gin.md#GIN-LIMIT)
[64.4.7. Examples](gin.md#GIN-EXAMPLES)
[64.5. BRIN Indexes](brin.md)
    

[64.5.1. Introduction](brin.md#BRIN-INTRO)
[64.5.2. Built-in Operator Classes](brin.md#BRIN-BUILTIN-OPCLASSES)
[64.5.3. Extensibility](brin.md#BRIN-EXTENSIBILITY)
[64.6. Hash Indexes](hash-index.md)
    

[64.6.1. Overview](hash-index.md#HASH-INTRO)
[64.6.2. Implementation](hash-index.md#HASH-IMPLEMENTATION)

* * *

[Prev](custom-rmgr.md "63.2. Custom WAL Resource Managers") | [Up](internals.md "Part VII. Internals")|  [Next](btree.md "64.1. B-Tree Indexes")  
---|---|---  
63.2. Custom WAL Resource Managers | [Home](index.md "PostgreSQL 17.5 Documentation")|  64.1. B-Tree Indexes
