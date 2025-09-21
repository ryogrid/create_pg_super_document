Chapter 4. SQL Syntax  
---  
[Prev](sql.md "Part II. The SQL Language") | [Up](sql.md "Part II. The SQL Language")| Part II. The SQL Language| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-syntax-lexical.md "4.1. Lexical Structure")  
  
* * *

## Chapter 4. SQL Syntax

**Table of Contents**

[4.1. Lexical Structure](sql-syntax-lexical.md)
    

[4.1.1. Identifiers and Key Words](sql-syntax-lexical.md#SQL-SYNTAX-IDENTIFIERS)
[4.1.2. Constants](sql-syntax-lexical.md#SQL-SYNTAX-CONSTANTS)
[4.1.3. Operators](sql-syntax-lexical.md#SQL-SYNTAX-OPERATORS)
[4.1.4. Special Characters](sql-syntax-lexical.md#SQL-SYNTAX-SPECIAL-CHARS)
[4.1.5. Comments](sql-syntax-lexical.md#SQL-SYNTAX-COMMENTS)
[4.1.6. Operator Precedence](sql-syntax-lexical.md#SQL-PRECEDENCE)
[4.2. Value Expressions](sql-expressions.md)
    

[4.2.1. Column References](sql-expressions.md#SQL-EXPRESSIONS-COLUMN-REFS)
[4.2.2. Positional Parameters](sql-expressions.md#SQL-EXPRESSIONS-PARAMETERS-POSITIONAL)
[4.2.3. Subscripts](sql-expressions.md#SQL-EXPRESSIONS-SUBSCRIPTS)
[4.2.4. Field Selection](sql-expressions.md#FIELD-SELECTION)
[4.2.5. Operator Invocations](sql-expressions.md#SQL-EXPRESSIONS-OPERATOR-CALLS)
[4.2.6. Function Calls](sql-expressions.md#SQL-EXPRESSIONS-FUNCTION-CALLS)
[4.2.7. Aggregate Expressions](sql-expressions.md#SYNTAX-AGGREGATES)
[4.2.8. Window Function Calls](sql-expressions.md#SYNTAX-WINDOW-FUNCTIONS)
[4.2.9. Type Casts](sql-expressions.md#SQL-SYNTAX-TYPE-CASTS)
[4.2.10. Collation Expressions](sql-expressions.md#SQL-SYNTAX-COLLATE-EXPRS)
[4.2.11. Scalar Subqueries](sql-expressions.md#SQL-SYNTAX-SCALAR-SUBQUERIES)
[4.2.12. Array Constructors](sql-expressions.md#SQL-SYNTAX-ARRAY-CONSTRUCTORS)
[4.2.13. Row Constructors](sql-expressions.md#SQL-SYNTAX-ROW-CONSTRUCTORS)
[4.2.14. Expression Evaluation Rules](sql-expressions.md#SYNTAX-EXPRESS-EVAL)
[4.3. Calling Functions](sql-syntax-calling-funcs.md)
    

[4.3.1. Using Positional Notation](sql-syntax-calling-funcs.md#SQL-SYNTAX-CALLING-FUNCS-POSITIONAL)
[4.3.2. Using Named Notation](sql-syntax-calling-funcs.md#SQL-SYNTAX-CALLING-FUNCS-NAMED)
[4.3.3. Using Mixed Notation](sql-syntax-calling-funcs.md#SQL-SYNTAX-CALLING-FUNCS-MIXED)

This chapter describes the syntax of SQL. It forms the foundation for understanding the following chapters which will go into detail about how SQL commands are applied to define and modify data. 

We also advise users who are already familiar with SQL to read this chapter carefully because it contains several rules and concepts that are implemented inconsistently among SQL databases or that are specific to PostgreSQL. 

* * *

[Prev](sql.md "Part II. The SQL Language") | [Up](sql.md "Part II. The SQL Language")|  [Next](sql-syntax-lexical.md "4.1. Lexical Structure")  
---|---|---  
Part II. The SQL Language | [Home](index.md "PostgreSQL 17.5 Documentation")|  4.1. Lexical Structure
