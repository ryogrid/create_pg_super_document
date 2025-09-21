41.4. Expressions  
---  
[Prev](plpgsql-declarations.md "41.3. Declarations") | [Up](plpgsql.md "Chapter 41. PL/pgSQL — SQL Procedural Language")| Chapter 41. PL/pgSQL — SQL Procedural Language| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](plpgsql-statements.md "41.5. Basic Statements")  
  
* * *

## 41.4. Expressions #

All expressions used in PL/pgSQL statements are processed using the server's main SQL executor. For example, when you write a PL/pgSQL statement like 
    
    
    IF _expression_ THEN ...
    

PL/pgSQL will evaluate the expression by feeding a query like 
    
    
    SELECT _expression_
    

to the main SQL engine. While forming the `SELECT` command, any occurrences of PL/pgSQL variable names are replaced by query parameters, as discussed in detail in [Section 41.11.1](plpgsql-implementation.md#PLPGSQL-VAR-SUBST "41.11.1. Variable Substitution"). This allows the query plan for the `SELECT` to be prepared just once and then reused for subsequent evaluations with different values of the variables. Thus, what really happens on first use of an expression is essentially a `PREPARE` command. For example, if we have declared two integer variables `x` and `y`, and we write 
    
    
    IF x < y THEN ...
    

what happens behind the scenes is equivalent to 
    
    
    PREPARE _statement_name_(integer, integer) AS SELECT $1 < $2;
    

and then this prepared statement is `EXECUTE`d for each execution of the `IF` statement, with the current values of the PL/pgSQL variables supplied as parameter values. Normally these details are not important to a PL/pgSQL user, but they are useful to know when trying to diagnose a problem. More information appears in [Section 41.11.2](plpgsql-implementation.md#PLPGSQL-PLAN-CACHING "41.11.2. Plan Caching"). 

Since an _`expression`_ is converted to a `SELECT` command, it can contain the same clauses that an ordinary `SELECT` would, except that it cannot include a top-level `UNION`, `INTERSECT`, or `EXCEPT` clause. Thus for example one could test whether a table is non-empty with 
    
    
    IF count(*) > 0 FROM my_table THEN ...
    

since the _`expression`_ between `IF` and `THEN` is parsed as though it were `SELECT count(*) > 0 FROM my_table`. The `SELECT` must produce a single column, and not more than one row. (If it produces no rows, the result is taken as NULL.) 

* * *

[Prev](plpgsql-declarations.md "41.3. Declarations") | [Up](plpgsql.md "Chapter 41. PL/pgSQL — SQL Procedural Language")|  [Next](plpgsql-statements.md "41.5. Basic Statements")  
---|---|---  
41.3. Declarations | [Home](index.md "PostgreSQL 17.5 Documentation")|  41.5. Basic Statements
