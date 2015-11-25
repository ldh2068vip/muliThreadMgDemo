 package com.srdb.migration.metadata;
 
 import java.util.ArrayList;

 public class ColumnList
 {
   private ArrayList<Column> list = null;
 
   public void add(Column column)
   {
     if (this.list == null) {
       this.list = new ArrayList();
     }
 
     this.list.add(column);
   }
 
   public int size() {
     return this.list != null ? this.list.size() : 0;
   }
 
   public Column get(int index) {
     return (Column)this.list.get(index);
   }
 
   public Column get(String colName)
   {
     for (Column column : this.list) {
       if (column.getName().equals(colName)) {
         return column;
       }
     }
 
     return null;
   }
 }
