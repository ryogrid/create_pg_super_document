3.2. Views  
---  
[Prev](tutorial-advanced-intro.md "3.1. Introduction") | [Up](tutorial-advanced.md "Chapter 3. Advanced Features")| Chapter 3. Advanced Features| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](tutorial-fk.md "3.3. Foreign Keys")  
  
* * *

## 3.2. Views #

Refer back to the queries in [Section 2.6](tutorial-join.md "2.6. Joins Between Tables"). Suppose the combined listing of weather records and city location is of particular interest to your application, but you do not want to type the query each time you need it. You can create a _view_ over the query, which gives a name to the query that you can refer to like an ordinary table: 
    
    
    CREATE VIEW myview AS
        SELECT name, temp_lo, temp_hi, prcp, date, location
            FROM weather, cities
            WHERE city = name;
    
    SELECT * FROM myview;
    

Making liberal use of views is a key aspect of good SQL database design. Views allow you to encapsulate the details of the structure of your tables, which might change as your application evolves, behind consistent interfaces. 

Views can be used in almost any place a real table can be used. Building views upon other views is not uncommon. 

* * *

[Prev](tutorial-advanced-intro.md "3.1. Introduction") | [Up](tutorial-advanced.md "Chapter 3. Advanced Features")|  [Next](tutorial-fk.md "3.3. Foreign Keys")  
---|---|---  
3.1. Introduction | [Home](index.md "PostgreSQL 17.5 Documentation")|  3.3. Foreign Keys
