Chapter 50. Overview of PostgreSQL Internals  
---  
[Prev](internals.md "Part VII. Internals") | [Up](internals.md "Part VII. Internals")| Part VII. Internals| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](query-path.md "50.1. The Path of a Query")  
  
* * *

## Chapter 50. Overview of PostgreSQL Internals

**Table of Contents**

[50.1. The Path of a Query](query-path.md)
[50.2. How Connections Are Established](connect-estab.md)
[50.3. The Parser Stage](parser-stage.md)
    

[50.3.1. Parser](parser-stage.md#PARSER-STAGE-PARSER)
[50.3.2. Transformation Process](parser-stage.md#PARSER-STAGE-TRANSFORMATION-PROCESS)
[50.4. The PostgreSQL Rule System](rule-system.md)
[50.5. Planner/Optimizer](planner-optimizer.md)
    

[50.5.1. Generating Possible Plans](planner-optimizer.md#PLANNER-OPTIMIZER-GENERATING-POSSIBLE-PLANS)
[50.6. Executor](executor.md)

### Author

This chapter originated as part of [[sim98]](biblio.md#SIM98 "Enhancement of the ANSI SQL Implementation of PostgreSQL") Stefan Simkovics' Master's Thesis prepared at Vienna University of Technology under the direction of O.Univ.Prof.Dr. Georg Gottlob and Univ.Ass. Mag. Katrin Seyr. 

This chapter gives an overview of the internal structure of the backend of PostgreSQL. After having read the following sections you should have an idea of how a query is processed. This chapter is intended to help the reader understand the general sequence of operations that occur within the backend from the point at which a query is received, to the point at which the results are returned to the client. 

* * *

[Prev](internals.md "Part VII. Internals") | [Up](internals.md "Part VII. Internals")|  [Next](query-path.md "50.1. The Path of a Query")  
---|---|---  
Part VII. Internals | [Home](index.md "PostgreSQL 17.5 Documentation")|  50.1. The Path of a Query
