Part V. Server Programming  
---  
[Prev](infoschema-views.md "35.66. views") | [Up](index.md "PostgreSQL 17.5 Documentation")| PostgreSQL 17.5 Documentation| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](extend.md "Chapter 36. Extending SQL")  
  
* * *

# Part V. Server Programming

This part is about extending the server functionality with user-defined functions, data types, triggers, etc. These are advanced topics which should be approached only after all the other user documentation about PostgreSQL has been understood. Later chapters in this part describe the server-side programming languages available in the PostgreSQL distribution as well as general issues concerning server-side programming. It is essential to read at least the earlier sections of [Chapter 36](extend.md "Chapter 36. Extending SQL") (covering functions) before diving into the material about server-side programming. 

**Table of Contents**

[36\. Extending SQL](extend.md)
    

[36.1. How Extensibility Works](extend-how.md)
[36.2. The PostgreSQL Type System](extend-type-system.md)
[36.3. User-Defined Functions](xfunc.md)
[36.4. User-Defined Procedures](xproc.md)
[36.5. Query Language (SQL) Functions](xfunc-sql.md)
[36.6. Function Overloading](xfunc-overload.md)
[36.7. Function Volatility Categories](xfunc-volatility.md)
[36.8. Procedural Language Functions](xfunc-pl.md)
[36.9. Internal Functions](xfunc-internal.md)
[36.10. C-Language Functions](xfunc-c.md)
[36.11. Function Optimization Information](xfunc-optimization.md)
[36.12. User-Defined Aggregates](xaggr.md)
[36.13. User-Defined Types](xtypes.md)
[36.14. User-Defined Operators](xoper.md)
[36.15. Operator Optimization Information](xoper-optimization.md)
[36.16. Interfacing Extensions to Indexes](xindex.md)
[36.17. Packaging Related Objects into an Extension](extend-extensions.md)
[36.18. Extension Building Infrastructure](extend-pgxs.md)
[37\. Triggers](triggers.md)
    

[37.1. Overview of Trigger Behavior](trigger-definition.md)
[37.2. Visibility of Data Changes](trigger-datachanges.md)
[37.3. Writing Trigger Functions in C](trigger-interface.md)
[37.4. A Complete Trigger Example](trigger-example.md)
[38\. Event Triggers](event-triggers.md)
    

[38.1. Overview of Event Trigger Behavior](event-trigger-definition.md)
[38.2. Event Trigger Firing Matrix](event-trigger-matrix.md)
[38.3. Writing Event Trigger Functions in C](event-trigger-interface.md)
[38.4. A Complete Event Trigger Example](event-trigger-example.md)
[38.5. A Table Rewrite Event Trigger Example](event-trigger-table-rewrite-example.md)
[38.6. A Database Login Event Trigger Example](event-trigger-database-login-example.md)
[39\. The Rule System](rules.md)
    

[39.1. The Query Tree](querytree.md)
[39.2. Views and the Rule System](rules-views.md)
[39.3. Materialized Views](rules-materializedviews.md)
[39.4. Rules on `INSERT`, `UPDATE`, and `DELETE`](rules-update.md)
[39.5. Rules and Privileges](rules-privileges.md)
[39.6. Rules and Command Status](rules-status.md)
[39.7. Rules Versus Triggers](rules-triggers.md)
[40\. Procedural Languages](xplang.md)
    

[40.1. Installing Procedural Languages](xplang-install.md)
[41\. PL/pgSQL — SQL Procedural Language](plpgsql.md)
    

[41.1. Overview](plpgsql-overview.md)
[41.2. Structure of PL/pgSQL](plpgsql-structure.md)
[41.3. Declarations](plpgsql-declarations.md)
[41.4. Expressions](plpgsql-expressions.md)
[41.5. Basic Statements](plpgsql-statements.md)
[41.6. Control Structures](plpgsql-control-structures.md)
[41.7. Cursors](plpgsql-cursors.md)
[41.8. Transaction Management](plpgsql-transactions.md)
[41.9. Errors and Messages](plpgsql-errors-and-messages.md)
[41.10. Trigger Functions](plpgsql-trigger.md)
[41.11. PL/pgSQL under the Hood](plpgsql-implementation.md)
[41.12. Tips for Developing in PL/pgSQL](plpgsql-development-tips.md)
[41.13. Porting from Oracle PL/SQL](plpgsql-porting.md)
[42\. PL/Tcl — Tcl Procedural Language](pltcl.md)
    

[42.1. Overview](pltcl-overview.md)
[42.2. PL/Tcl Functions and Arguments](pltcl-functions.md)
[42.3. Data Values in PL/Tcl](pltcl-data.md)
[42.4. Global Data in PL/Tcl](pltcl-global.md)
[42.5. Database Access from PL/Tcl](pltcl-dbaccess.md)
[42.6. Trigger Functions in PL/Tcl](pltcl-trigger.md)
[42.7. Event Trigger Functions in PL/Tcl](pltcl-event-trigger.md)
[42.8. Error Handling in PL/Tcl](pltcl-error-handling.md)
[42.9. Explicit Subtransactions in PL/Tcl](pltcl-subtransactions.md)
[42.10. Transaction Management](pltcl-transactions.md)
[42.11. PL/Tcl Configuration](pltcl-config.md)
[42.12. Tcl Procedure Names](pltcl-procnames.md)
[43\. PL/Perl — Perl Procedural Language](plperl.md)
    

[43.1. PL/Perl Functions and Arguments](plperl-funcs.md)
[43.2. Data Values in PL/Perl](plperl-data.md)
[43.3. Built-in Functions](plperl-builtins.md)
[43.4. Global Values in PL/Perl](plperl-global.md)
[43.5. Trusted and Untrusted PL/Perl](plperl-trusted.md)
[43.6. PL/Perl Triggers](plperl-triggers.md)
[43.7. PL/Perl Event Triggers](plperl-event-triggers.md)
[43.8. PL/Perl Under the Hood](plperl-under-the-hood.md)
[44\. PL/Python — Python Procedural Language](plpython.md)
    

[44.1. PL/Python Functions](plpython-funcs.md)
[44.2. Data Values](plpython-data.md)
[44.3. Sharing Data](plpython-sharing.md)
[44.4. Anonymous Code Blocks](plpython-do.md)
[44.5. Trigger Functions](plpython-trigger.md)
[44.6. Database Access](plpython-database.md)
[44.7. Explicit Subtransactions](plpython-subtransaction.md)
[44.8. Transaction Management](plpython-transactions.md)
[44.9. Utility Functions](plpython-util.md)
[44.10. Python 2 vs. Python 3](plpython-python23.md)
[44.11. Environment Variables](plpython-envar.md)
[45\. Server Programming Interface](spi.md)
    

[45.1. Interface Functions](spi-interface.md)
[45.2. Interface Support Functions](spi-interface-support.md)
[45.3. Memory Management](spi-memory.md)
[45.4. Transaction Management](spi-transaction.md)
[45.5. Visibility of Data Changes](spi-visibility.md)
[45.6. Examples](spi-examples.md)
[46\. Background Worker Processes](bgworker.md)
[47\. Logical Decoding](logicaldecoding.md)
    

[47.1. Logical Decoding Examples](logicaldecoding-example.md)
[47.2. Logical Decoding Concepts](logicaldecoding-explanation.md)
[47.3. Streaming Replication Protocol Interface](logicaldecoding-walsender.md)
[47.4. Logical Decoding SQL Interface](logicaldecoding-sql.md)
[47.5. System Catalogs Related to Logical Decoding](logicaldecoding-catalogs.md)
[47.6. Logical Decoding Output Plugins](logicaldecoding-output-plugin.md)
[47.7. Logical Decoding Output Writers](logicaldecoding-writer.md)
[47.8. Synchronous Replication Support for Logical Decoding](logicaldecoding-synchronous.md)
[47.9. Streaming of Large Transactions for Logical Decoding](logicaldecoding-streaming.md)
[47.10. Two-phase Commit Support for Logical Decoding](logicaldecoding-two-phase-commits.md)
[48\. Replication Progress Tracking](replication-origins.md)
[49\. Archive Modules](archive-modules.md)
    

[49.1. Initialization Functions](archive-module-init.md)
[49.2. Archive Module Callbacks](archive-module-callbacks.md)

* * *

[Prev](infoschema-views.md "35.66. views") | [Up](index.md "PostgreSQL 17.5 Documentation")|  [Next](extend.md "Chapter 36. Extending SQL")  
---|---|---  
35.66. `views` | [Home](index.md "PostgreSQL 17.5 Documentation")|  Chapter 36. Extending SQL
