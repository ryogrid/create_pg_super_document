Chapter 10. Type Conversion  
---  
[Prev](functions-statistics.md "9.31. Statistics Information Functions") | [Up](sql.md "Part II. The SQL Language")| Part II. The SQL Language| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](typeconv-overview.md "10.1. Overview")  
  
* * *

## Chapter 10. Type Conversion

**Table of Contents**

[10.1. Overview](typeconv-overview.md)
[10.2. Operators](typeconv-oper.md)
[10.3. Functions](typeconv-func.md)
[10.4. Value Storage](typeconv-query.md)
[10.5. `UNION`, `CASE`, and Related Constructs](typeconv-union-case.md)
[10.6. `SELECT` Output Columns](typeconv-select.md)

SQL statements can, intentionally or not, require the mixing of different data types in the same expression. PostgreSQL has extensive facilities for evaluating mixed-type expressions. 

In many cases a user does not need to understand the details of the type conversion mechanism. However, implicit conversions done by PostgreSQL can affect the results of a query. When necessary, these results can be tailored by using _explicit_ type conversion. 

This chapter introduces the PostgreSQL type conversion mechanisms and conventions. Refer to the relevant sections in [Chapter 8](datatype.md "Chapter 8. Data Types") and [Chapter 9](functions.md "Chapter 9. Functions and Operators") for more information on specific data types and allowed functions and operators. 

* * *

[Prev](functions-statistics.md "9.31. Statistics Information Functions") | [Up](sql.md "Part II. The SQL Language")|  [Next](typeconv-overview.md "10.1. Overview")  
---|---|---  
9.31. Statistics Information Functions | [Home](index.md "PostgreSQL 17.5 Documentation")|  10.1. Overview
