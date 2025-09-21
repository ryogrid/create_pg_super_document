Chapter 59. Writing a Custom Scan Provider  
---  
[Prev](tablesample-support-functions.md "58.1. Sampling Method Support Functions") | [Up](internals.md "Part VII. Internals")| Part VII. Internals| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](custom-scan-path.md "59.1. Creating Custom Scan Paths")  
  
* * *

## Chapter 59. Writing a Custom Scan Provider

**Table of Contents**

[59.1. Creating Custom Scan Paths](custom-scan-path.md)
    

[59.1.1. Custom Scan Path Callbacks](custom-scan-path.md#CUSTOM-SCAN-PATH-CALLBACKS)
[59.2. Creating Custom Scan Plans](custom-scan-plan.md)
    

[59.2.1. Custom Scan Plan Callbacks](custom-scan-plan.md#CUSTOM-SCAN-PLAN-CALLBACKS)
[59.3. Executing Custom Scans](custom-scan-execution.md)
    

[59.3.1. Custom Scan Execution Callbacks](custom-scan-execution.md#CUSTOM-SCAN-EXECUTION-CALLBACKS)

PostgreSQL supports a set of experimental facilities which are intended to allow extension modules to add new scan types to the system. Unlike a [foreign data wrapper](fdwhandler.md "Chapter 57. Writing a Foreign Data Wrapper"), which is only responsible for knowing how to scan its own foreign tables, a custom scan provider can provide an alternative method of scanning any relation in the system. Typically, the motivation for writing a custom scan provider will be to allow the use of some optimization not supported by the core system, such as caching or some form of hardware acceleration. This chapter outlines how to write a new custom scan provider. 

Implementing a new type of custom scan is a three-step process. First, during planning, it is necessary to generate access paths representing a scan using the proposed strategy. Second, if one of those access paths is selected by the planner as the optimal strategy for scanning a particular relation, the access path must be converted to a plan. Finally, it must be possible to execute the plan and generate the same results that would have been generated for any other access path targeting the same relation. 

* * *

[Prev](tablesample-support-functions.md "58.1. Sampling Method Support Functions") | [Up](internals.md "Part VII. Internals")|  [Next](custom-scan-path.md "59.1. Creating Custom Scan Paths")  
---|---|---  
58.1. Sampling Method Support Functions | [Home](index.md "PostgreSQL 17.5 Documentation")|  59.1. Creating Custom Scan Paths
