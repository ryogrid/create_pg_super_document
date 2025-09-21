7.4. Combining Queries (`UNION`, `INTERSECT`, `EXCEPT`)  
---  
[Prev](queries-select-lists.md "7.3. Select Lists") | [Up](queries.md "Chapter 7. Queries")| Chapter 7. Queries| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](queries-order.md "7.5. Sorting Rows \(ORDER BY\)")  
  
* * *

## 7.4. Combining Queries (`UNION`, `INTERSECT`, `EXCEPT`) #

The results of two queries can be combined using the set operations union, intersection, and difference. The syntax is 
    
    
    _query1_ UNION [ALL] _query2_
    _query1_ INTERSECT [ALL] _query2_
    _query1_ EXCEPT [ALL] _query2_
    

where _`query1`_ and _`query2`_ are queries that can use any of the features discussed up to this point. 

`UNION` effectively appends the result of _`query2`_ to the result of _`query1`_ (although there is no guarantee that this is the order in which the rows are actually returned). Furthermore, it eliminates duplicate rows from its result, in the same way as `DISTINCT`, unless `UNION ALL` is used. 

`INTERSECT` returns all rows that are both in the result of _`query1`_ and in the result of _`query2`_. Duplicate rows are eliminated unless `INTERSECT ALL` is used. 

`EXCEPT` returns all rows that are in the result of _`query1`_ but not in the result of _`query2`_. (This is sometimes called the _difference_ between two queries.) Again, duplicates are eliminated unless `EXCEPT ALL` is used. 

In order to calculate the union, intersection, or difference of two queries, the two queries must be “union compatible”, which means that they return the same number of columns and the corresponding columns have compatible data types, as described in [Section 10.5](typeconv-union-case.md "10.5. UNION, CASE, and Related Constructs"). 

Set operations can be combined, for example 
    
    
    _query1_ UNION _query2_ EXCEPT _query3_
    

which is equivalent to 
    
    
    (_query1_ UNION _query2_) EXCEPT _query3_
    

As shown here, you can use parentheses to control the order of evaluation. Without parentheses, `UNION` and `EXCEPT` associate left-to-right, but `INTERSECT` binds more tightly than those two operators. Thus 
    
    
    _query1_ UNION _query2_ INTERSECT _query3_
    

means 
    
    
    _query1_ UNION (_query2_ INTERSECT _query3_)
    

You can also surround an individual _`query`_ with parentheses. This is important if the _`query`_ needs to use any of the clauses discussed in following sections, such as `LIMIT`. Without parentheses, you'll get a syntax error, or else the clause will be understood as applying to the output of the set operation rather than one of its inputs. For example, 
    
    
    SELECT a FROM b UNION SELECT x FROM y LIMIT 10
    

is accepted, but it means 
    
    
    (SELECT a FROM b UNION SELECT x FROM y) LIMIT 10
    

not 
    
    
    SELECT a FROM b UNION (SELECT x FROM y LIMIT 10)
    

* * *

[Prev](queries-select-lists.md "7.3. Select Lists") | [Up](queries.md "Chapter 7. Queries")|  [Next](queries-order.md "7.5. Sorting Rows \(ORDER BY\)")  
---|---|---  
7.3. Select Lists | [Home](index.md "PostgreSQL 17.5 Documentation")|  7.5. Sorting Rows (`ORDER BY`)
