2.9. Deletions  
---  
[Prev](tutorial-update.md "2.8. Updates") | [Up](tutorial-sql.md "Chapter 2. The SQL Language")| Chapter 2. The SQL Language| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](tutorial-advanced.md "Chapter 3. Advanced Features")  
  
* * *

## 2.9. Deletions #

Rows can be removed from a table using the `DELETE` command. Suppose you are no longer interested in the weather of Hayward. Then you can do the following to delete those rows from the table: 
    
    
    DELETE FROM weather WHERE city = 'Hayward';
    

All weather records belonging to Hayward are removed. 
    
    
    SELECT * FROM weather;
    
    
    
         city      | temp_lo | temp_hi | prcp |    date
    ---------------+---------+---------+------+------------
     San Francisco |      46 |      50 | 0.25 | 1994-11-27
     San Francisco |      41 |      55 |    0 | 1994-11-29
    (2 rows)
    

One should be wary of statements of the form 
    
    
    DELETE FROM _tablename_ ;
    

Without a qualification, `DELETE` will remove _all_ rows from the given table, leaving it empty. The system will not request confirmation before doing this! 

* * *

[Prev](tutorial-update.md "2.8. Updates") | [Up](tutorial-sql.md "Chapter 2. The SQL Language")|  [Next](tutorial-advanced.md "Chapter 3. Advanced Features")  
---|---|---  
2.8. Updates | [Home](index.md "PostgreSQL 17.5 Documentation")|  Chapter 3. Advanced Features
