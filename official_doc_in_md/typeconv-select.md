10.6. `SELECT` Output Columns  
---  
[Prev](typeconv-union-case.md "10.5. UNION, CASE, and Related Constructs") | [Up](typeconv.md "Chapter 10. Type Conversion")| Chapter 10. Type Conversion| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](indexes.md "Chapter 11. Indexes")  
  
* * *

## 10.6. `SELECT` Output Columns #

The rules given in the preceding sections will result in assignment of non-`unknown` data types to all expressions in an SQL query, except for unspecified-type literals that appear as simple output columns of a `SELECT` command. For example, in 
    
    
    SELECT 'Hello World';
    

there is nothing to identify what type the string literal should be taken as. In this situation PostgreSQL will fall back to resolving the literal's type as `text`. 

When the `SELECT` is one arm of a `UNION` (or `INTERSECT` or `EXCEPT`) construct, or when it appears within `INSERT ... SELECT`, this rule is not applied since rules given in preceding sections take precedence. The type of an unspecified-type literal can be taken from the other `UNION` arm in the first case, or from the destination column in the second case. 

`RETURNING` lists are treated the same as `SELECT` output lists for this purpose. 

### Note

Prior to PostgreSQL 10, this rule did not exist, and unspecified-type literals in a `SELECT` output list were left as type `unknown`. That had assorted bad consequences, so it's been changed. 

* * *

[Prev](typeconv-union-case.md "10.5. UNION, CASE, and Related Constructs") | [Up](typeconv.md "Chapter 10. Type Conversion")|  [Next](indexes.md "Chapter 11. Indexes")  
---|---|---  
10.5. `UNION`, `CASE`, and Related Constructs | [Home](index.md "PostgreSQL 17.5 Documentation")|  Chapter 11. Indexes
