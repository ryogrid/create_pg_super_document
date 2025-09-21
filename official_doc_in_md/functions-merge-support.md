9.23. Merge Support Functions  
---  
[Prev](functions-window.md "9.22. Window Functions") | [Up](functions.md "Chapter 9. Functions and Operators")| Chapter 9. Functions and Operators| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](functions-subquery.md "9.24. Subquery Expressions")  
  
* * *

## 9.23. Merge Support Functions #

PostgreSQL includes one merge support function that may be used in the `RETURNING` list of a [MERGE](sql-merge.md "MERGE") command to identify the action taken for each row; see [Table 9.66](functions-merge-support.md#FUNCTIONS-MERGE-SUPPORT-TABLE "Table 9.66. Merge Support Functions"). 

**Table 9.66. Merge Support Functions**

Function  Description   
---  
`merge_action` ( ) → `text` Returns the merge action command executed for the current row. This will be `'INSERT'`, `'UPDATE'`, or `'DELETE'`.   
  
  


Example: 
    
    
    MERGE INTO products p
      USING stock s ON p.product_id = s.product_id
      WHEN MATCHED AND s.quantity > 0 THEN
        UPDATE SET in_stock = true, quantity = s.quantity
      WHEN MATCHED THEN
        UPDATE SET in_stock = false, quantity = 0
      WHEN NOT MATCHED THEN
        INSERT (product_id, in_stock, quantity)
          VALUES (s.product_id, true, s.quantity)
      RETURNING merge_action(), p.*;
    
     merge_action | product_id | in_stock | quantity
    --------------+------------+----------+----------
     UPDATE       |       1001 | t        |       50
     UPDATE       |       1002 | f        |        0
     INSERT       |       1003 | t        |       10
    

Note that this function can only be used in the `RETURNING` list of a `MERGE` command. It is an error to use it in any other part of a query. 

* * *

[Prev](functions-window.md "9.22. Window Functions") | [Up](functions.md "Chapter 9. Functions and Operators")|  [Next](functions-subquery.md "9.24. Subquery Expressions")  
---|---|---  
9.22. Window Functions | [Home](index.md "PostgreSQL 17.5 Documentation")|  9.24. Subquery Expressions
