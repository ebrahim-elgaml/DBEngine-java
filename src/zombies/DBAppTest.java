package zombies;
import java.awt.RenderingHints.Key;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Enumeration;
import java.util.Hashtable;

import org.apache.jdbm.BTree;
import org.apache.jdbm.DBAbstract;
import org.apache.jdbm.DBMaker;
import org.apache.jdbm.DBStore;


public class DBAppTest {
	
	public void testBPTree() throws IOException 
	{
		DBAbstract db;
        BTree bTree = null;
        File file = new File("testBTree" + ".ser");
        db = (DBStore) DBMaker.openFile("testBTree" + ".ser").disableCache().make();
        bTree = BTree.createInstance(db); 
           
	}
	public void testDBApp() throws DBAppException, FileNotFoundException, IOException, ClassNotFoundException {
		DBApp myApp = new DBApp(); 
		Hashtable<String,String> htblColNameType = new Hashtable<String, String>();
		Hashtable<String,String> htblColNameRefs = new Hashtable<String, String>();
		htblColNameType.put("name", "String");
		htblColNameType.put("age", "integer");
		htblColNameType.put("ID", "integer");
		htblColNameType.put("dep_id", "integer");
		myApp.createTable("student",htblColNameType, htblColNameRefs, "ID");
		//for (int i= 0; i <myApp.tables.size(); i++) {
			//System.out.println(myApp.tables.get(i).name);
		//}
		Hashtable <String,String> htblColNameTy = new Hashtable<String, String>(); 
		htblColNameTy.put("ID", "String");
		htblColNameTy.put("name", "String");
		myApp.createMultiDimIndex("student", htblColNameTy);
		
		//Enumeration<String> colNames = myApp.tables.get(0).index.keys();
		/*while(colNames.hasMoreElements()) { 
			String col = colNames.nextElement(); 
			System.out.println(myApp.tables.get(0).index.get(col) + "," +col );
		}*/
		Hashtable<String,String> htblColNameValue = new Hashtable<String, String>();
		htblColNameValue.put("age", "20");
		htblColNameValue.put("name", "ebrahim");
		htblColNameValue.put("ID", "0000");
		htblColNameValue.put("dep_id", "00001");
		
		//htblColNameValue.put("hopa", "00000");
		myApp.insertIntoTable("student", htblColNameValue);
		
		//System.out.println(myApp.tables.get(0).toString());
		htblColNameValue = new Hashtable<String, String>();
		htblColNameValue.put("age", "19");
		htblColNameValue.put("name", "myriame");
		htblColNameValue.put("ID", "00001");
		htblColNameValue.put("dep_id", "00001");
		myApp.insertIntoTable("student", htblColNameValue);
		//System.out.println(myApp.tables.get(0).toString());
		
		htblColNameValue = new Hashtable<String, String>();
		htblColNameValue.put("age", "20");
		htblColNameValue.put("name", "maggie");
		htblColNameValue.put("ID", "00002");
		htblColNameValue.put("dep_id", "00001");
		
		myApp.insertIntoTable("student", htblColNameValue);
		//System.out.println(myApp.tables.get(0).toString());
		//System.out.println(myApp.tables.get(0).Btree.get(0).toString());
		myApp.createIndex("student", "name");
	 
		//myApp.createTable("student",htblColNameType, htblColNameRefs, "ID");
		//System.out.println(myApp.tables.get(0).partitioned.get(0));
		Hashtable<String,String> htblColNameValu = new Hashtable<String,String>();
		htblColNameValu.put("name", "ebrahim");
		myApp.selectFromTable("student", htblColNameValu, "OR");
	}
			
	public static void main(String []args) throws IOException, DBAppException, ClassNotFoundException {
		DBAppTest test = new DBAppTest();
		//test.testBPTree(); 
		test.testDBApp();
	}
}
