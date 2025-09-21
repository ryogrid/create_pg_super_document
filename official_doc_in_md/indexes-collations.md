11.11. Indexes and Collations  
---  
[Prev](indexes-opclass.md "11.10. Operator Classes and Operator Families") | [Up](indexes.md "Chapter 11. Indexes")| Chapter 11. Indexes| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](indexes-examine.md "11.12. Examining Index Usage")  
  
* * *

## 11.11. Indexes and Collations #

An index can support only one collation per index column. If multiple collations are of interest, multiple indexes may be needed. 

Consider these statements: 
    
    
    CREATE TABLE test1c (
        id integer,
        content varchar COLLATE "x"
    );
    
    CREATE INDEX test1c_content_index ON test1c (content);
    

The index automatically uses the collation of the underlying column. So a query of the form 
    
    
    SELECT * FROM test1c WHERE content > _constant_ ;
    

could use the index, because the comparison will by default use the collation of the column. However, this index cannot accelerate queries that involve some other collation. So if queries of the form, say, 
    
    
    SELECT * FROM test1c WHERE content > _constant_ COLLATE "y";
    

are also of interest, an additional index could be created that supports the `"y"` collation, like this: 
    
    
    CREATE INDEX test1c_content_y_index ON test1c (content COLLATE "y");
    

* * *

[Prev](indexes-opclass.md "11.10. Operator Classes and Operator Families") | [Up](indexes.md "Chapter 11. Indexes")|  [Next](indexes-examine.md "11.12. Examining Index Usage")  
---|---|---  
11.10. Operator Classes and Operator Families | [Home](index.md "PostgreSQL 17.5 Documentation")|  11.12. Examining Index Usage
