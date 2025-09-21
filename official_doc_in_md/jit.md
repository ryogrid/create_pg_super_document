Chapter 30. Just-in-Time Compilation (JIT)  
---  
[Prev](logical-replication-quick-setup.md "29.12. Quick Setup") | [Up](admin.md "Part III. Server Administration")| Part III. Server Administration| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](jit-reason.md "30.1. What Is JIT compilation?")  
  
* * *

## Chapter 30. Just-in-Time Compilation (JIT)

**Table of Contents**

[30.1. What Is JIT compilation?](jit-reason.md)
    

[30.1.1. JIT Accelerated Operations](jit-reason.md#JIT-ACCELERATED-OPERATIONS)
[30.1.2. Inlining](jit-reason.md#JIT-INLINING)
[30.1.3. Optimization](jit-reason.md#JIT-OPTIMIZATION)
[30.2. When to JIT?](jit-decision.md)
[30.3. Configuration](jit-configuration.md)
[30.4. Extensibility](jit-extensibility.md)
    

[30.4.1. Inlining Support for Extensions](jit-extensibility.md#JIT-EXTENSIBILITY-BITCODE)
[30.4.2. Pluggable JIT Providers](jit-extensibility.md#JIT-PLUGGABLE)

This chapter explains what just-in-time compilation is, and how it can be configured in PostgreSQL. 

* * *

[Prev](logical-replication-quick-setup.md "29.12. Quick Setup") | [Up](admin.md "Part III. Server Administration")|  [Next](jit-reason.md "30.1. What Is JIT compilation?")  
---|---|---  
29.12. Quick Setup | [Home](index.md "PostgreSQL 17.5 Documentation")|  30.1. What Is JIT compilation?
