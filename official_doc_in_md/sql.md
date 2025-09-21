Part II. The SQL Language  
---  
[Prev](tutorial-conclusion.md "3.7. Conclusion") | [Up](index.md "PostgreSQL 17.5 Documentation")| PostgreSQL 17.5 Documentation| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-syntax.md "Chapter 4. SQL Syntax")  
  
* * *

# Part II. The SQL Language

This part describes the use of the SQL language in PostgreSQL. We start with describing the general syntax of SQL, then how to create tables, how to populate the database, and how to query it. The middle part lists the available data types and functions for use in SQL commands. Lastly, we address several aspects of importance for tuning a database. 

The information is arranged so that a novice user can follow it from start to end and gain a full understanding of the topics without having to refer forward too many times. The chapters are intended to be self-contained, so that advanced users can read the chapters individually as they choose. The information is presented in narrative form with topical units. Readers looking for a complete description of a particular command are encouraged to review the [Part VI](reference.md "Part VI. Reference"). 

Readers should know how to connect to a PostgreSQL database and issue SQL commands. Readers that are unfamiliar with these issues are encouraged to read [Part I](tutorial.md "Part I. Tutorial") first. SQL commands are typically entered using the PostgreSQL interactive terminal psql, but other programs that have similar functionality can be used as well. 

**Table of Contents**

[4\. SQL Syntax](sql-syntax.md)
    

[4.1. Lexical Structure](sql-syntax-lexical.md)
[4.2. Value Expressions](sql-expressions.md)
[4.3. Calling Functions](sql-syntax-calling-funcs.md)
[5\. Data Definition](ddl.md)
    

[5.1. Table Basics](ddl-basics.md)
[5.2. Default Values](ddl-default.md)
[5.3. Identity Columns](ddl-identity-columns.md)
[5.4. Generated Columns](ddl-generated-columns.md)
[5.5. Constraints](ddl-constraints.md)
[5.6. System Columns](ddl-system-columns.md)
[5.7. Modifying Tables](ddl-alter.md)
[5.8. Privileges](ddl-priv.md)
[5.9. Row Security Policies](ddl-rowsecurity.md)
[5.10. Schemas](ddl-schemas.md)
[5.11. Inheritance](ddl-inherit.md)
[5.12. Table Partitioning](ddl-partitioning.md)
[5.13. Foreign Data](ddl-foreign-data.md)
[5.14. Other Database Objects](ddl-others.md)
[5.15. Dependency Tracking](ddl-depend.md)
[6\. Data Manipulation](dml.md)
    

[6.1. Inserting Data](dml-insert.md)
[6.2. Updating Data](dml-update.md)
[6.3. Deleting Data](dml-delete.md)
[6.4. Returning Data from Modified Rows](dml-returning.md)
[7\. Queries](queries.md)
    

[7.1. Overview](queries-overview.md)
[7.2. Table Expressions](queries-table-expressions.md)
[7.3. Select Lists](queries-select-lists.md)
[7.4. Combining Queries (`UNION`, `INTERSECT`, `EXCEPT`)](queries-union.md)
[7.5. Sorting Rows (`ORDER BY`)](queries-order.md)
[7.6. `LIMIT` and `OFFSET`](queries-limit.md)
[7.7. `VALUES` Lists](queries-values.md)
[7.8. `WITH` Queries (Common Table Expressions)](queries-with.md)
[8\. Data Types](datatype.md)
    

[8.1. Numeric Types](datatype-numeric.md)
[8.2. Monetary Types](datatype-money.md)
[8.3. Character Types](datatype-character.md)
[8.4. Binary Data Types](datatype-binary.md)
[8.5. Date/Time Types](datatype-datetime.md)
[8.6. Boolean Type](datatype-boolean.md)
[8.7. Enumerated Types](datatype-enum.md)
[8.8. Geometric Types](datatype-geometric.md)
[8.9. Network Address Types](datatype-net-types.md)
[8.10. Bit String Types](datatype-bit.md)
[8.11. Text Search Types](datatype-textsearch.md)
[8.12. UUID Type](datatype-uuid.md)
[8.13. XML Type](datatype-xml.md)
[8.14. JSON Types](datatype-json.md)
[8.15. Arrays](arrays.md)
[8.16. Composite Types](rowtypes.md)
[8.17. Range Types](rangetypes.md)
[8.18. Domain Types](domains.md)
[8.19. Object Identifier Types](datatype-oid.md)
[8.20. `pg_lsn` Type](datatype-pg-lsn.md)
[8.21. Pseudo-Types](datatype-pseudo.md)
[9\. Functions and Operators](functions.md)
    

[9.1. Logical Operators](functions-logical.md)
[9.2. Comparison Functions and Operators](functions-comparison.md)
[9.3. Mathematical Functions and Operators](functions-math.md)
[9.4. String Functions and Operators](functions-string.md)
[9.5. Binary String Functions and Operators](functions-binarystring.md)
[9.6. Bit String Functions and Operators](functions-bitstring.md)
[9.7. Pattern Matching](functions-matching.md)
[9.8. Data Type Formatting Functions](functions-formatting.md)
[9.9. Date/Time Functions and Operators](functions-datetime.md)
[9.10. Enum Support Functions](functions-enum.md)
[9.11. Geometric Functions and Operators](functions-geometry.md)
[9.12. Network Address Functions and Operators](functions-net.md)
[9.13. Text Search Functions and Operators](functions-textsearch.md)
[9.14. UUID Functions](functions-uuid.md)
[9.15. XML Functions](functions-xml.md)
[9.16. JSON Functions and Operators](functions-json.md)
[9.17. Sequence Manipulation Functions](functions-sequence.md)
[9.18. Conditional Expressions](functions-conditional.md)
[9.19. Array Functions and Operators](functions-array.md)
[9.20. Range/Multirange Functions and Operators](functions-range.md)
[9.21. Aggregate Functions](functions-aggregate.md)
[9.22. Window Functions](functions-window.md)
[9.23. Merge Support Functions](functions-merge-support.md)
[9.24. Subquery Expressions](functions-subquery.md)
[9.25. Row and Array Comparisons](functions-comparisons.md)
[9.26. Set Returning Functions](functions-srf.md)
[9.27. System Information Functions and Operators](functions-info.md)
[9.28. System Administration Functions](functions-admin.md)
[9.29. Trigger Functions](functions-trigger.md)
[9.30. Event Trigger Functions](functions-event-triggers.md)
[9.31. Statistics Information Functions](functions-statistics.md)
[10\. Type Conversion](typeconv.md)
    

[10.1. Overview](typeconv-overview.md)
[10.2. Operators](typeconv-oper.md)
[10.3. Functions](typeconv-func.md)
[10.4. Value Storage](typeconv-query.md)
[10.5. `UNION`, `CASE`, and Related Constructs](typeconv-union-case.md)
[10.6. `SELECT` Output Columns](typeconv-select.md)
[11\. Indexes](indexes.md)
    

[11.1. Introduction](indexes-intro.md)
[11.2. Index Types](indexes-types.md)
[11.3. Multicolumn Indexes](indexes-multicolumn.md)
[11.4. Indexes and `ORDER BY`](indexes-ordering.md)
[11.5. Combining Multiple Indexes](indexes-bitmap-scans.md)
[11.6. Unique Indexes](indexes-unique.md)
[11.7. Indexes on Expressions](indexes-expressional.md)
[11.8. Partial Indexes](indexes-partial.md)
[11.9. Index-Only Scans and Covering Indexes](indexes-index-only-scans.md)
[11.10. Operator Classes and Operator Families](indexes-opclass.md)
[11.11. Indexes and Collations](indexes-collations.md)
[11.12. Examining Index Usage](indexes-examine.md)
[12\. Full Text Search](textsearch.md)
    

[12.1. Introduction](textsearch-intro.md)
[12.2. Tables and Indexes](textsearch-tables.md)
[12.3. Controlling Text Search](textsearch-controls.md)
[12.4. Additional Features](textsearch-features.md)
[12.5. Parsers](textsearch-parsers.md)
[12.6. Dictionaries](textsearch-dictionaries.md)
[12.7. Configuration Example](textsearch-configuration.md)
[12.8. Testing and Debugging Text Search](textsearch-debugging.md)
[12.9. Preferred Index Types for Text Search](textsearch-indexes.md)
[12.10. psql Support](textsearch-psql.md)
[12.11. Limitations](textsearch-limitations.md)
[13\. Concurrency Control](mvcc.md)
    

[13.1. Introduction](mvcc-intro.md)
[13.2. Transaction Isolation](transaction-iso.md)
[13.3. Explicit Locking](explicit-locking.md)
[13.4. Data Consistency Checks at the Application Level](applevel-consistency.md)
[13.5. Serialization Failure Handling](mvcc-serialization-failure-handling.md)
[13.6. Caveats](mvcc-caveats.md)
[13.7. Locking and Indexes](locking-indexes.md)
[14\. Performance Tips](performance-tips.md)
    

[14.1. Using `EXPLAIN`](using-explain.md)
[14.2. Statistics Used by the Planner](planner-stats.md)
[14.3. Controlling the Planner with Explicit `JOIN` Clauses](explicit-joins.md)
[14.4. Populating a Database](populate.md)
[14.5. Non-Durable Settings](non-durability.md)
[15\. Parallel Query](parallel-query.md)
    

[15.1. How Parallel Query Works](how-parallel-query-works.md)
[15.2. When Can Parallel Query Be Used?](when-can-parallel-query-be-used.md)
[15.3. Parallel Plans](parallel-plans.md)
[15.4. Parallel Safety](parallel-safety.md)

* * *

[Prev](tutorial-conclusion.md "3.7. Conclusion") | [Up](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-syntax.md "Chapter 4. SQL Syntax")  
---|---|---  
3.7. Conclusion | [Home](index.md "PostgreSQL 17.5 Documentation")|  Chapter 4. SQL Syntax
