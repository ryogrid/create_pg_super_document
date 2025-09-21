F.21. lo — manage large objects  
---  
[Prev](isn.md "F.20. isn — data types for international standard numbers \(ISBN, EAN, UPC, etc.\)") | [Up](contrib.md "Appendix F. Additional Supplied Modules and Extensions")| Appendix F. Additional Supplied Modules and Extensions| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](ltree.md "F.22. ltree — hierarchical tree-like data type")  
  
* * *

## F.21. lo — manage large objects #

[F.21.1. Rationale](lo.md#LO-RATIONALE)
[F.21.2. How to Use It](lo.md#LO-HOW-TO-USE)
[F.21.3. Limitations](lo.md#LO-LIMITATIONS)
[F.21.4. Author](lo.md#LO-AUTHOR)

The `lo` module provides support for managing Large Objects (also called LOs or BLOBs). This includes a data type `lo` and a trigger `lo_manage`. 

This module is considered “trusted”, that is, it can be installed by non-superusers who have `CREATE` privilege on the current database. 

### F.21.1. Rationale #

One of the problems with the JDBC driver (and this affects the ODBC driver also), is that the specification assumes that references to BLOBs (Binary Large OBjects) are stored within a table, and if that entry is changed, the associated BLOB is deleted from the database. 

As PostgreSQL stands, this doesn't occur. Large objects are treated as objects in their own right; a table entry can reference a large object by OID, but there can be multiple table entries referencing the same large object OID, so the system doesn't delete the large object just because you change or remove one such entry. 

Now this is fine for PostgreSQL-specific applications, but standard code using JDBC or ODBC won't delete the objects, resulting in orphan objects — objects that are not referenced by anything, and simply occupy disk space. 

The `lo` module allows fixing this by attaching a trigger to tables that contain LO reference columns. The trigger essentially just does a `lo_unlink` whenever you delete or modify a value referencing a large object. When you use this trigger, you are assuming that there is only one database reference to any large object that is referenced in a trigger-controlled column! 

The module also provides a data type `lo`, which is really just a [_[domain](glossary.md#GLOSSARY-DOMAIN "Domain")_](glossary.md#GLOSSARY-DOMAIN) over the `oid` type. This is useful for differentiating database columns that hold large object references from those that are OIDs of other things. You don't have to use the `lo` type to use the trigger, but it may be convenient to use it to keep track of which columns in your database represent large objects that you are managing with the trigger. It is also rumored that the ODBC driver gets confused if you don't use `lo` for BLOB columns. 

### F.21.2. How to Use It #

Here's a simple example of usage: 
    
    
    CREATE TABLE image (title text, raster lo);
    
    CREATE TRIGGER t_raster BEFORE UPDATE OR DELETE ON image
        FOR EACH ROW EXECUTE FUNCTION lo_manage(raster);
    

For each column that will contain unique references to large objects, create a `BEFORE UPDATE OR DELETE` trigger, and give the column name as the sole trigger argument. You can also restrict the trigger to only execute on updates to the column by using `BEFORE UPDATE OF` _`column_name`_. If you need multiple `lo` columns in the same table, create a separate trigger for each one, remembering to give a different name to each trigger on the same table. 

### F.21.3. Limitations #

  * Dropping a table will still orphan any objects it contains, as the trigger is not executed. You can avoid this by preceding the `DROP TABLE` with `DELETE FROM _`table`_`. 

`TRUNCATE` has the same hazard. 

If you already have, or suspect you have, orphaned large objects, see the [vacuumlo](vacuumlo.md "vacuumlo") module to help you clean them up. It's a good idea to run vacuumlo occasionally as a back-stop to the `lo_manage` trigger. 

  * Some frontends may create their own tables, and will not create the associated trigger(s). Also, users may not remember (or know) to create the triggers. 




### F.21.4. Author #

Peter Mount `<[peter@retep.org.uk](mailto:peter@retep.org.uk)>`

* * *

[Prev](isn.md "F.20. isn — data types for international standard numbers \(ISBN, EAN, UPC, etc.\)") | [Up](contrib.md "Appendix F. Additional Supplied Modules and Extensions")|  [Next](ltree.md "F.22. ltree — hierarchical tree-like data type")  
---|---|---  
F.20. isn — data types for international standard numbers (ISBN, EAN, UPC, etc.) | [Home](index.md "PostgreSQL 17.5 Documentation")|  F.22. ltree — hierarchical tree-like data type
