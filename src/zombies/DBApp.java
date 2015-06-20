package zombies;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.jdbm.*;
import org.apache.jdbm.*;
public class DBApp {

    ArrayList<Table> tables; 
    static String tableOpened = null;
    static int pageOpened = -1;
    static int numberOfRecords;
    static int numberInNodes;
    static PrintWriter writer;
    static ArrayList<ArrayList> csv;
    public DBApp () {
    	tables = new ArrayList<Table>(); 
        csv = new ArrayList<ArrayList>();
    	
    	try {
			writer = new PrintWriter("metadata.csv","UTF-8");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
	public void createTable(String strTableName,  Hashtable<String,String> htblColNameType,  
			Hashtable<String,String>htblColNameRefs,  String strKeyColName) throws DBAppException {
		// check if Table name already exists 
		for (int i=0; i <tables.size() ; i++) {
			Table t = tables.get(i); 
			if(t.name.equals(strTableName)) {
				//System.out.println("table already exists");
				throw new DBAppException("table already exists");
			}		
		}
		if(strKeyColName == null || strKeyColName == "" || !htblColNameType.containsKey(strKeyColName)) {
			System.out.println("invalid primary key");
			throw new DBAppException("invalid primary key");
		}
		//if (htblColNameType.containsValue(null) || (htblColNameRefs.containsValue(null) && htblColNameRefs.size() >0) || strTableName == null || strTableName == "") {
			//System.out.println("invalid arg");
			//throw new DBAppException("invalid arg"); 
		//}
		tableOpened = strTableName;
		Table table = new Table (strTableName, htblColNameType, htblColNameRefs, strKeyColName,numberOfRecords);
		tables.add(table);
		//table.Btree.add(strKeyColName); 
		createIndex(strTableName, strKeyColName);
        csv = new ArrayList<ArrayList>();
    	
    	try {
			writer = new PrintWriter("metadata.csv","UTF-8");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		tableOpened = null;
		}
	public void createMultiDimIndex(String strTableName,
			Hashtable<String,String> htblColNames ) throws DBAppException
			{
		
		String col = "";
		boolean found = false; //table
		Table table = new Table(); 
		for (int i=0; i <tables.size() ; i++) {
			Table t = tables.get(i); 
			if(t.name.equals(strTableName)) { 
				found = true; 
				table = t;  
				tableOpened = table.name;
				pageOpened = table.pageNum;
				}
		}
		if(!found) {  
			System.out.println("table doesn't exist");
			tableOpened = null;
			pageOpened = -1;
			throw new DBAppException("table doesn't exist");
		}
		//hashed before
		found = false;
		Enumeration<String> second = htblColNames.keys();
		ArrayList<String> newColNames = new ArrayList<String>();
		while (second.hasMoreElements()){
			newColNames.add(second.nextElement());
		}
		for(int i =0;i<table.partitioned.size()&&!found;i++)
		{
			Enumeration<String> first = table.partitioned.get(i).htblColNames.keys();
			ArrayList<String> originalTableNames = new ArrayList<String>();
			while (first.hasMoreElements()){
				originalTableNames.add(first.nextElement());
			}
			if(newColNames.size()==originalTableNames.size()){
				if(newColNames.containsAll(originalTableNames)){
					found = true;
				}
			}
		}
		if(found){
			System.out.println("alreadyHashedBefor");
			tableOpened = null;
			pageOpened = -1;
			return;
		}
		MultiDimension pHashTable = new MultiDimension(htblColNames);
		//
		int k = 0 ;
		Enumeration<String> colNames = htblColNames.keys();
		while (colNames.hasMoreElements()){// a loop to create the hashtable for each colomn
			String colName = colNames.nextElement();
			col+=colName+",";
			int colIndex = table.index.get(colName);
			pHashTable.hashedIndex.add(colIndex);
			for(int i = 0; i <= table.pagesSoFar; i++) {
		    	Page p = (Page)table.open( table.name +"," + i );
		    	if (p == null) break; 
		        for(int j = 0; j <p.records.size() ; j++) {
		        	ArrayList<Comparable> curr = p.records.removeFirst();
		        	Comparable x = curr.get(colIndex);
		        	String hash = pHashTable.linearHash(x);
		        	//System.out.println("Hashed here"+ hash +"and the key is " +x.toString());
		        	pHashTable.hashtables.get(k).put(x,hash);
		            p.records.addLast(curr);
		        }
		    }	
		}
		
		col = col.substring(0,col.length()-1);//discarding last char
	    for(int i = 0; i <= table.pagesSoFar; i++) {
	    	Page p = (Page)table.open( table.name +"," + i );
	    	if (p == null) break; 
	    	for(int j = 0; j <p.records.size() ; j++) {
	        	String hashed="";
	        	ArrayList<Comparable> curr = p.records.removeFirst();
	        	//System.out.println(pHashTable.hashedIndex.size());
	        	for(int c = 0 ;c<pHashTable.hashedIndex.size();c++){
	        		//System.out.println("col Ind"+pHashTable.hashedIndex.get(c));
	        		Comparable x = curr.get(pHashTable.hashedIndex.get(c));
	        		hashed += pHashTable.linearHash(x);
	        		//hashed+=pHashTable.hashtables.get(c).get(x);
	        	}
	            p.records.addLast(curr);
	            pHashTable.insert(i,j, hashed);
	        }
	    }	    
	    try {
	    	
        	ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(new File(strTableName +"," +col)));
			oos.writeObject(pHashTable);
			oos.close();
			tableOpened = null;
			pageOpened = -1;
		} catch (IOException e) {
			
		}
	    table.partitioned.add(pHashTable);
	    Enumeration<String> colIndexNames = htblColNames.keys();
	    while(colIndexNames.hasMoreElements()) {
	    	String strColName = colIndexNames.nextElement();
	    	for(int i = 0;i<csv.size();i++) {
	    		if(csv.get(i).contains((Object)strTableName)&&csv.get(i).contains((Object)strColName))
	    			csv.get(i).add(4, "true");
        	
	    	}
	    }
        try {
			writer = new PrintWriter("metadata.csv","UTF-8");
			for(int i = 0;i<csv.size();i++) {
				writer.println(csv.get(i).get(0)+","+csv.get(i).get(1)+","+
			csv.get(i).get(2)+","+csv.get(i).get(3)+","+csv.get(i).get(4)+csv.get(i).get(5));
			}
			writer.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void createIndex(String strTableName,   String strColName) throws DBAppException {
		// check if table exists 
		boolean found = false; 
		Table table = new Table(); 
		for (int i=0; i <tables.size() ; i++) {
			Table t = tables.get(i); 
			if(t.name.equals(strTableName)) { 
				found = true; 
				table = t; 
				tableOpened = table.name;
				pageOpened = table.pageNum;
			}
		}
		if(!found) {  
			System.out.println("table doesn't exist");
			throw new DBAppException("table doesn't exist"); 
	}
		// check col exists in the table 
		if (!table.col.containsKey(strColName)) {
			//System.out.println("3ib keda");
			tableOpened = null;
			pageOpened = -1;
			throw new DBAppException("colName doesn't exist");
		}
		if (table.Btree.contains(strColName)) {
			System.out.println("index already exists");
			tableOpened = null;
			pageOpened = -1;
			return;
		} // 3ameli warning 3'arib 
		DBAbstract db;
        BTree bTree = null;
        File file = new File(strTableName +"," +strColName + ".ser.t");
        db = (DBStore) DBMaker.openFile(strTableName +"," +strColName + ".ser.t").disableCache().make();
        try {
        bTree = BTree.createInstance(db);
        ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(new File(strTableName +"," +strColName + ".ser")));
        oos.writeObject(bTree);
        oos.close();
}
        catch(Exception e) {
        	
        }
		int colIndex = table.index.get(strColName); 
	    for(int i = 0; i <= table.pagesSoFar; i++) {
	    	Page p = (Page)table.open(table.name +"," + i );
	    	if (p == null) break; 
	        for(int j = 0; j <p.records.size() ; j++) {
	        	ArrayList<Comparable> curr = p.records.removeFirst();
	        	Comparable x = curr.get(colIndex); 
	        	try {
					bTree.insert(x, table.name +"," + i+ "," + j, false);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
	            p.records.addLast(curr);
	        }
	    }
	    try {
        	ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(new File(strTableName +"," +strColName + ".ser")));
			oos.writeObject(bTree);
			oos.close();
			tableOpened = null;
			pageOpened = -1;
		} catch (IOException e) {
		}
        table.Btree.add(strColName);
        for(int i = 0;i<csv.size();i++) {
        	if(csv.get(i).contains((Object)strTableName)&&csv.get(i).contains((Object)strColName))
        		csv.get(i).add(4, "true");
        	
        }
        try {
			writer = new PrintWriter("metadata.csv","UTF-8");
			for(int i = 0;i<csv.size();i++) {
				writer.println(csv.get(i).get(0)+","+csv.get(i).get(1)+","+
			csv.get(i).get(2)+","+csv.get(i).get(3)+","+csv.get(i).get(4)+csv.get(i).get(5));
			}
			writer.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	//exception 
	//insert new row at bottom ?
	public void insertIntoTable(String strTableName,   
			Hashtable<String,String> htblColNameValue) throws DBAppException {
		// check if table name exists 
		boolean found = false; 
		Table table = new Table(); 
		for (int i=0; i <tables.size() ; i++) {
			Table t = tables.get(i); 
			if(t.name.equals(strTableName)) { 
				found = true; 
				table = t;  
				tableOpened = table.name;
				pageOpened = table.pageNum;
			}
		}
		if(!found) {  
			System.out.println("table doesn't exist");
			throw new DBAppException("table doesn't exist"); }
		// check if col exists in table 
		Enumeration<String> colNames = htblColNameValue.keys();
		while (colNames.hasMoreElements()) {
			Comparable colName = colNames.nextElement();
	
			if (!table.col.containsKey(colName)) {
				tableOpened = null;
				pageOpened = -1;
				//System.out.println(colName + "is not a col in table" + table.name);
				throw new DBAppException(colName + " is not a col in table" + table.name);
			}
	}
	boolean check = table.checkInsertionConstraints(htblColNameValue);
	if(!check) {
		tableOpened = null;
		pageOpened = -1;
		return; 
	}
	// insert into table 
	boolean inserted = table.insertIntoTable(htblColNameValue);
	if (!inserted) {
		System.out.println("7asal 7aaga wesha msh 3arefeh heya eh bs "
				+ "el insertion msh da5al bs enta zy el fol 3'albn i 3'altetna e7na");
		tableOpened = null;
		pageOpened = -1;
		return; 
	}
	
	
	
}
	public void deleteFromTable(String strTableName,
			Hashtable<String, String> htblColNameValue, String strOperator)
			throws DBEngineException {
		// checking
		// if table exists
		boolean found = false;
		Table table = new Table();
		for (int i = 0; i < tables.size(); i++) {
			Table t = tables.get(i);
			if (t.name.equals(strTableName)) {
				found = true;
				table = t;
				tableOpened = table.name;
				pageOpened = table.pageNum;
			}
		}
		if (!found) {
			System.out.println("table doesn't exist");
			throw new DBEngineException("table doesn't exist");
		}
		// checking if column exists
		Enumeration<String> colNames = htblColNameValue.keys();
		while (colNames.hasMoreElements()) {
			Comparable colName = colNames.nextElement();

			if (!table.col.containsKey(colName)) {
				System.out.println(colName + "is not a column in table"
						+ table.name);
				tableOpened = null;
				pageOpened = -1;
				throw new DBEngineException(colName
						+ "is not a column in table" + table.name);
			}

		}
		// search for row
		int deleted = table.deleteFromTable(htblColNameValue, strOperator);
		if (deleted == 0) {
			System.out
					.println("Ana ma3reftesh a3mel delete lesabab msh 3arfah");
			throw new DBEngineException(
					"Ana ma3reftesh a3mel delete lesabab msh 3arfah");
		} else
			System.out.println("deletion is completed successfully, " + deleted
					+ " records were deleted");
		tableOpened = null;
		pageOpened = -1;
		return;

	}

public static ArrayList<String> EnumtoString(Enumeration<String> e){
		ArrayList<String> r = new ArrayList<String>();
		while(e.hasMoreElements()){
			r.add(e.nextElement());
		}
		return r;
	}

	public void init() {
		this.numberOfRecords = 200;
		this.numberInNodes = 20;
	}
	public Object open(String tableName) {
		//String pageName = this.name + "," + pageNum; 
		ObjectInputStream ois;
		Object p = null; 
		try {
			ois = new ObjectInputStream(new FileInputStream(new File(tableName + ".ser")));
			p = ois.readObject();
			ois.close();
		} catch (Exception e) { 
	
	}
		return p; 
	}
	public void saveAll() {
		if(tableOpened != null ) {
			 try {
				 
				 Table t = (Table)this.open(tableOpened);
				 ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(new File(tableOpened +"," +pageOpened + ".ser")));
				 oos.writeObject(t);
				 oos.close();
				 tableOpened = null;
					pageOpened = -1;
			} catch (IOException e) {
			}
			
		}
	}
	public Iterator  selectFromTable(String strTable,
			Hashtable<String, String> htblColNameValue, String strOperator) {
		boolean found = false;
		Table t = tables.get(0);
		int tableIndex = 0;
		for (int i = 0; i < tables.size(); i++) {
			if (tables.get(i).name.equals(strTable)) {
				found = true;
				tableIndex = i;
				break;
			}
		}
		if (!found) {
			System.out.println("Table Not Found");
			return null;
		}
		t = tables.get(tableIndex);
		found = false;
		Enumeration<String> tName = t.col.keys();
		ArrayList<String> tableColNames = new ArrayList<String>();// list of col
																	// names
		Enumeration<String> colName = htblColNameValue.keys();
		ArrayList<String> inColNames = new ArrayList<String>();// list of the
																// given
																// parameter
																// names
		while (colName.hasMoreElements()) {
			inColNames.add(colName.nextElement());
			//System.out.println(inColNames.get(0));
		}
		while (tName.hasMoreElements()) {
			tableColNames.add(tName.nextElement());
		}
		LinkedList<ArrayList<Comparable>> result = new LinkedList<ArrayList<Comparable>>();
		ArrayList<Pair> foundPairs = new ArrayList<Pair>();
		if (!tableColNames.containsAll(inColNames)) {
			System.out.print("This col Does Not Exist In The Database");
			return null;
		}
		if (inColNames.size() == 1) {
			if (t.Btree.contains(inColNames.get(0))) {
				BTree bTree = null;
				String res = "";

				try {
				DBAbstract db = (DBAbstract) DBMaker.openFile(t.name + "," + inColNames.get(0) + ".ser.t").disableCache().make();	
				ObjectInputStream ois = new ObjectInputStream(new FileInputStream(new File(t.name + "," + inColNames.get(0) + ".ser")));
		        bTree = (BTree)ois.readObject();
		        ois.close();
		        res = (String) bTree.get(htblColNameValue.get(inColNames.get(0)));
				}
				catch (Exception e) {}
				// search in B+tree
				//DBAbstract db;
		        		        /*BTree bTree = null;
		        //File file = new File(t.name + "," + inColNames.get(0) + ".ser");
		        db = (DBStore) DBMaker.openFile(t.name + "," + inColNames.get(0) + ".ser").disableCache().make();
		        try {
		        bTree = BTree.createInstance(db); 
		        */
				//res = (String) bTree.get(htblColNameValue.get(inColNames.get(0)));
				//if(res == null)
					//System.out.println("null");
				//}
		        //catch(Exception e) {
		        	
		        //}
		        
		        
				String[] y = res.split(",");
				int index = Integer.parseInt(y[1]);
				Page resPage = (Page) t.open(t.name + "," + y[0]);
				ArrayList<Comparable> results = null;
				for (int p = 0; p < resPage.records.size(); p++) {
					if (p == index)
						results = resPage.records.getFirst();
					resPage.records.addLast(resPage.records.removeFirst());

				}
				Iterator<Comparable> it = results.iterator();
				return it;
			} else {
				LinkedList<ArrayList<Comparable>> r = t.linearSearch(
						htblColNameValue, strOperator);
				return r.iterator();
			}

		} else {
			ArrayList<Integer> ind = new ArrayList<Integer>();
			for (int h = 0; h < t.partitioned.size(); h++) {
				ind.add(t.partitioned.get(h).howManyColIndexed(
						EnumtoString(htblColNameValue.keys())));

			}
			if (t.getMaxIndex(ind) != -1) {
				return t.search(htblColNameValue, strOperator).iterator();
			} else {
				return t.linearSearch(htblColNameValue, strOperator).iterator();
			}

		}
	}

	public ArrayList<Pair> searchAndGetPair(String strTable,
			Hashtable<String, String> htblColNameValue, String strOperator) {
		boolean found = false;
		Table t = tables.get(0);
		int tableIndex = 0;
		for (int i = 0; i < tables.size(); i++) {
			if (tables.get(i).name.equals(strTable)) {
				found = true;
				tableIndex = i;
				break;
			}
		}
		if (!found) {
			System.out.println("Table Not Found");
			return null;
		}
		t = tables.get(tableIndex);
		found = false;
		Enumeration<String> tName = t.col.keys();
		ArrayList<String> tableColNames = new ArrayList<String>();// list of col
																	// names
		Enumeration<String> colName = htblColNameValue.keys();
		ArrayList<String> inColNames = new ArrayList<String>();// list of the
																// given
																// parameter
																// names
		while (colName.hasMoreElements()) {
			inColNames.add(colName.nextElement());
		}
		while (tName.hasMoreElements()) {
			tableColNames.add(tName.nextElement());
		}
		LinkedList<ArrayList<Comparable>> result = new LinkedList<ArrayList<Comparable>>();
		ArrayList<Pair> foundPairs = new ArrayList<Pair>();
		if (!tableColNames.containsAll(inColNames)) {
			System.out.print("This col Does Not Exist In The Database");
			return null;
		}
		if (inColNames.size() == 1) {
			if (t.Btree.contains(inColNames.get(0))) {
				// search in B+tree
				DBAbstract db;
		        BTree bTree = null;
		        File file = new File(t.name + "," + inColNames.get(0)+ ".ser");
		        db = (DBStore) DBMaker.openFile(t.name + "," + inColNames.get(0)+ ".ser").disableCache().make();
		        String res = "";
		        try{
		        bTree = BTree.createInstance(db); 
		        
				res = (String) bTree.get(htblColNameValue
						.get(inColNames.get(0)));
		        }
		        catch(Exception e) {
		        	
		        }
				String[] y = res.split(",");
				int index = Integer.parseInt(y[1]);
				Page resPage = (Page) t.open(t.name + "," + y[0]);
				ArrayList<Comparable> results = null;
				for (int p = 0; p < resPage.records.size(); p++) {
					if (p == index)
						results = resPage.records.getFirst();
					resPage.records.addLast(resPage.records.removeFirst());

				}
				ArrayList<Pair> aaaa = new ArrayList<Pair>();
				Pair p = new Pair(Integer.parseInt(y[0]),
						Integer.parseInt(y[1]));
				aaaa.add(p);
				return aaaa;
			} else {
				return t.linearSearchAndGetPair(htblColNameValue, strOperator);
				
			}

		} else {
			ArrayList<Integer> ind = new ArrayList<Integer>();
			for (int h = 0; h < t.partitioned.size(); h++) {
				ind.add(t.partitioned.get(h).howManyColIndexed(
						EnumtoString(htblColNameValue.keys())));

			}
			if (t.getMaxIndex(ind) != -1) {
				return t.searchAndGetPair(htblColNameValue, strOperator);
			} else {
				return t.linearSearchAndGetPair(htblColNameValue, strOperator);
			}

		}
	}

}
	


