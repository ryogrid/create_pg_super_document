Chapter 39. The Rule System  
---  
[Prev](event-trigger-database-login-example.md "38.6. A Database Login Event Trigger Example") | [Up](server-programming.md "Part V. Server Programming")| Part V. Server Programming| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](querytree.md "39.1. The Query Tree")  
  
* * *

## Chapter 39. The Rule System

**Table of Contents**

[39.1. The Query Tree](querytree.md)
[39.2. Views and the Rule System](rules-views.md)
    

[39.2.1. How `SELECT` Rules Work](rules-views.md#RULES-SELECT)
[39.2.2. View Rules in Non-`SELECT` Statements](rules-views.md#RULES-VIEWS-NON-SELECT)
[39.2.3. The Power of Views in PostgreSQL](rules-views.md#RULES-VIEWS-POWER)
[39.2.4. Updating a View](rules-views.md#RULES-VIEWS-UPDATE)
[39.3. Materialized Views](rules-materializedviews.md)
[39.4. Rules on `INSERT`, `UPDATE`, and `DELETE`](rules-update.md)
    

[39.4.1. How Update Rules Work](rules-update.md#RULES-UPDATE-HOW)
[39.4.2. Cooperation with Views](rules-update.md#RULES-UPDATE-VIEWS)
[39.5. Rules and Privileges](rules-privileges.md)
[39.6. Rules and Command Status](rules-status.md)
[39.7. Rules Versus Triggers](rules-triggers.md)

This chapter discusses the rule system in PostgreSQL. Production rule systems are conceptually simple, but there are many subtle points involved in actually using them. 

Some other database systems define active database rules, which are usually stored procedures and triggers. In PostgreSQL, these can be implemented using functions and triggers as well. 

The rule system (more precisely speaking, the query rewrite rule system) is totally different from stored procedures and triggers. It modifies queries to take rules into consideration, and then passes the modified query to the query planner for planning and execution. It is very powerful, and can be used for many things such as query language procedures, views, and versions. The theoretical foundations and the power of this rule system are also discussed in [[ston90b]](biblio.md#STON90B) and [[ong90]](biblio.md#ONG90). 

* * *

[Prev](event-trigger-database-login-example.md "38.6. A Database Login Event Trigger Example") | [Up](server-programming.md "Part V. Server Programming")|  [Next](querytree.md "39.1. The Query Tree")  
---|---|---  
38.6. A Database Login Event Trigger Example | [Home](index.md "PostgreSQL 17.5 Documentation")|  39.1. The Query Tree
