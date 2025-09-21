Chapter 38. Event Triggers  
---  
[Prev](trigger-example.md "37.4. A Complete Trigger Example") | [Up](server-programming.md "Part V. Server Programming")| Part V. Server Programming| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](event-trigger-definition.md "38.1. Overview of Event Trigger Behavior")  
  
* * *

## Chapter 38. Event Triggers

**Table of Contents**

[38.1. Overview of Event Trigger Behavior](event-trigger-definition.md)
[38.2. Event Trigger Firing Matrix](event-trigger-matrix.md)
[38.3. Writing Event Trigger Functions in C](event-trigger-interface.md)
[38.4. A Complete Event Trigger Example](event-trigger-example.md)
[38.5. A Table Rewrite Event Trigger Example](event-trigger-table-rewrite-example.md)
[38.6. A Database Login Event Trigger Example](event-trigger-database-login-example.md)

To supplement the trigger mechanism discussed in [Chapter 37](triggers.md "Chapter 37. Triggers"), PostgreSQL also provides event triggers. Unlike regular triggers, which are attached to a single table and capture only DML events, event triggers are global to a particular database and are capable of capturing DDL events. 

Like regular triggers, event triggers can be written in any procedural language that includes event trigger support, or in C, but not in plain SQL. 

* * *

[Prev](trigger-example.md "37.4. A Complete Trigger Example") | [Up](server-programming.md "Part V. Server Programming")|  [Next](event-trigger-definition.md "38.1. Overview of Event Trigger Behavior")  
---|---|---  
37.4. A Complete Trigger Example | [Home](index.md "PostgreSQL 17.5 Documentation")|  38.1. Overview of Event Trigger Behavior
