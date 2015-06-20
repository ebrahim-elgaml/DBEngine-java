package zombies;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Hashtable;








import java.util.LinkedList;

import org.apache.jdbm.*;

public class Table {
	//ArrayList<String> pages;
	// name , type 
	Hashtable<String,String> col;
	// name of col and table 
	Hashtable<String,String> ref;
	// name of col , number 
	Hashtable<String, Integer> index;
	String primary;
	String name;
	ArrayList<String> Btree; 
	//ArrayList<String> Hash; 
	int pagesSoFar; 
	ArrayList<MultiDimension> partitioned = new ArrayList<MultiDimension>();
	int pageNum;
	int NumberinPage;
	
	public Table ()  {
		
	}
	public Table (String name, Hashtable<String,String> colNameType, Hashtable<String,String> ref
			       ,String primary, int Number) {
		
		this.name = name; 
		this.col = colNameType;
		this.ref = ref; 
		this.primary = primary; 
		index = new Hashtable<String, Integer>();
		//Page p = new Page(); 
		//pages.add(p);
		Btree = new ArrayList<String>();
		//Hash = new ArrayList<String>();
		pagesSoFar = -1;
		Enumeration<String> colName = colNameType.keys();
		int i =0; 
		pageNum = -1;
		while(colName.hasMoreElements()) {
			//System.out.println(colName.nextElement());
		    index.put(colName.nextElement(), i); 
			i++;
		}
		NumberinPage = Number;
		
	}
	// NB : lw el tuple kol 7aga fih et3amalaha delete el tuples in the page must be decremented
	// 7assa eni mafrod a-roll back transaction f el catch aw 7aga zy keda
	// f warning 3'arib
	// m7tagin n-add multi kaman 
	public boolean insertIntoTable(Hashtable<String,String> htblColNameValue) throws DBAppException {
		//checkInsertionConstraints(htblColNameValue); 
		boolean inserted = true; 
		Page p = (Page)this.open(this.name + "," + this.pagesSoFar); 
		Page newPage; 
		if (p == null || p.records.size() == NumberinPage) { 
			pagesSoFar++; 
			pageNum = pagesSoFar;
			newPage = new Page();
		}
		else newPage = p; 
		Enumeration<String> colNames = htblColNameValue.keys();
		ArrayList<Comparable> newRecord = new ArrayList<Comparable>();
		for(int i = 0; i < this.col.size(); i++) {
			newRecord.add(i, 0);
		}
		while(colNames.hasMoreElements()) {
			String colName = colNames.nextElement(); 
			int colIndex = this.index.get(colName);
			newRecord.add(colIndex, htblColNameValue.get(colName));
		}
		newPage.tuples ++; 
		newPage.records.addLast(newRecord);
		
		
		try {
			//pageNum = -1;
			ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(new File(this.name + "," + this.pagesSoFar + ".ser")));
			oos.writeObject(newPage);
			oos.close();
		} catch (IOException e){ 
			inserted = false; 
		}
		if (inserted) {
			insertInBtree (htblColNameValue , newPage.tuples);
			insertIntoPartitioned(htblColNameValue, newPage.tuples);
		}

		return inserted; 
	}
	public void insertInBtree (Hashtable<String,String> htblColNameValue , int tupleNum) {
		Enumeration<String> colNames2 = htblColNameValue.keys();
		String place = this.pagesSoFar + "," + tupleNum; 
		while(colNames2.hasMoreElements()) {
			String colName = colNames2.nextElement(); 
			if (this.Btree.contains(colName)) {
				//BTree<String, String> bTree = (BTree<String, String>)open(this.name + "," + colName); 
				DBAbstract db = (DBAbstract) DBMaker.openFile(this.name + "," + colName + ".ser.t").disableCache().make();

                BTree<String, Serializable> bTree;
				try {
					//DBAbstract db = (DBAbstract) DBMaker.openFile(t.name + "," + inColNames.get(0) + ".ser.t").disableCache().make();	
					//ObjectInputStream2 ois = new ObjectInputStream2(new FileInputStream(new File(this.name + "," + colName + ".ser")));
			        
					//bTree = (BTree)ois.readObject();
			        //ois.close();
			          
					bTree = BTree.createInstance(db);
					
					ObjectOutputStream2 oos = new ObjectOutputStream2(new FileOutputStream(new File(this.name + "," + colName + ".ser")));
			        oos.writeObject(bTree);
			        oos.close();
			        bTree.insert(htblColNameValue.get(colName), place, false);
			      //bTree.insert(htblColNameValue.get(colName), place, false);
			        //ObjectOutputStream oos2 = new ObjectOutputStream(new FileOutputStream(new File(this.name + "," + colName + ".ser")));
			        //oos2.writeObject(bTree);
			        //oos2.close();

			        db.close();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
		}
	}
	public static ArrayList<String> EnumtoString(Enumeration<String> e) {
		ArrayList<String> r = new ArrayList<String>();
		while (e.hasMoreElements()) {
			r.add(e.nextElement());
		}
		return r;
	}

	public void insertIntoPartitioned(
			Hashtable<String, String> htblColNameValue, int tupleNum) {
		Enumeration<String> colNames = htblColNameValue.keys();
		ArrayList<String> names = EnumtoString(colNames);
		Pair p = new Pair(this.pagesSoFar, tupleNum);
		// hashed array corresponding to the order of hashing to handle nulls
		for (int i = 0; i < partitioned.size(); i++) {
			MultiDimension m = partitioned.get(i);
			String[] hashed = new String[m.hashtables.size()];
			// ArrayList<String> s = EnumtoString(m.htblColNames.keys());
			for (int j = 0; j < names.size(); j++) {
				int index = m.getHashedColIndexInHashTable(names.get(j));
				if (index != -1) {
					Hashtable<Comparable, String> h = m.hashtables.get(index);
					if (h.containsKey(htblColNameValue.get(names.get(j)))) {
						hashed[index] = h
								.get(htblColNameValue.get(names.get(j)));
					} else {
						h.put(htblColNameValue.get(names.get(j)), m
								.linearHash(htblColNameValue.get(names.get(j))));
						hashed[index] = h
								.get(htblColNameValue.get(names.get(j)));
					}
				}

			}
			String hashed2 = "";
			for (int k = 0; k < hashed.length; k++) {
				if (hashed[k] != null) {
					hashed2 += hashed[k];
				} else {
					hashed2 += "0000";
				}
			}
			m.insert(pageNum, p.rowNumber, hashed2);
		}
	}

	public ArrayList<Integer> getIndexHashed(ArrayList<String> colNames) {// search
																			// for
																			// the
																			// index
																			// if
																			// not
																			// found
																			// return
																			// -1
		ArrayList<Integer> r = new ArrayList<Integer>();
		for (int i = 0; i < partitioned.size(); i++) {
			if (partitioned.get(i).found(colNames)) {
				r.add(i);
			}
		}
		return r;
	}

	public int deleteFromTable(Hashtable<String, String> colNameValue,
			String strOperator) throws DBEngineException {
		boolean deleted = false;
		int count = 0;
		ArrayList<Pair> location = this.searchAndGetPair(colNameValue,
				strOperator);
		if (location != null) {
			for (int i = 0; i < location.size(); i++) {
				Page page = (Page) this.open(this.name + ","
						+ location.get(i).pageNumber);
				pageNum = location.get(i).pageNumber;
				if (page == null)
					return count;
				int tupleLocation = location.get(i).rowNumber;
				ArrayList<Comparable> record = page.records.get(tupleLocation);
				Enumeration<String> colNames = index.keys();
				String colName = "";
				for (int j = 0; i < record.size(); j++) {
					while (colNames.hasMoreElements()) {
						String temp = colNames.nextElement();
						int tempInt = index.get(temp);
						if (tempInt == j) {
							colName = temp;
							break;
						}
					}
					if (Btree.contains((Object) colName)) {
						DBAbstract db = (DBAbstract) DBMaker.openFile(this.name + "," + colName).disableCache().make();

		                BTree<String, Serializable> bTree;
		                try {
							bTree = BTree.createInstance(db);
							bTree.remove((String) record.get(j));
		                } catch (IOException e) {
							e.printStackTrace();
						}

						//BPTree myTree = (BPTree) open(this.name + "," + colName);
					
					}
					Pair deletePair = location.get(i);
					for (int k = 0; k < partitioned.size(); k++) {
						partitioned.get(k).removePair(deletePair);
					}
				}
				page.tombStone.add(tupleLocation, true);
				page.tuples--;
				count++;
				try {
					pageNum = -1;
					ObjectOutputStream oos = new ObjectOutputStream(
							new FileOutputStream(new File(this.name + ","
									+ this.pagesSoFar + ".ser")));
					oos.writeObject(page);
					oos.close();
				} catch (IOException e) {
					System.out.println("Error while writing data");
				}
			}
		} else {
			throw new DBEngineException("No record with this operator");
		}
		return count;
	}


	public boolean checkInsertionConstraints(Hashtable<String,String> htblColNameValue) throws DBAppException { 
		// check primary key is not null 
		if(!htblColNameValue.containsKey(this.primary) || htblColNameValue.get(this.primary) == null || htblColNameValue.get(this.primary) == "") {
			System.out.println("primary key cannot be null");
			throw new DBAppException("primary key cannot be null");
		}
		// check primary key is not repeated 
		int colIndex = this.index.get(this.primary); 
		Comparable value = htblColNameValue.get(this.primary);
		//System.out.println(value);
		boolean duplicated = false; 
		for(int i = 0; i <= this.pagesSoFar ; i++) {
			Page p = (Page)open(this.name +"," + i); 
			if(p == null) {
				//System.out.println("ba-break");
				break; 
			}
			for(int j = 0; j <p.records.size() ; j++) {
	        	ArrayList<Comparable> curr = p.records.removeFirst();
	        	Comparable x = curr.get(colIndex);
	        	//System.out.println(x);
	            p.records.addLast(curr);
	            if (x.compareTo(value) == 0){
	            	System.out.println("primary key already exists");
	            	duplicated = true; 
	            	throw new DBAppException("primary key already exists"); 
	            }
	        }
			if (duplicated) 
				return false; 
		}
		return true; 
	}
	public int getMaxIndex(ArrayList<Integer> x) {
		int i = 1;
		if (x == null || x.size() == 0)
			return -1;

		int max = x.get(0);
		int maxIndex = 0;
		do {
			if(i==x.size()){
				break;
			}
			if (max < x.get(i).intValue()) {
				max = x.get(i);
				maxIndex = i;
			}
		} while (i < x.size());
		return maxIndex;
	}

	public ArrayList<Pair> searchAndGetPair(
			Hashtable<String, String> htbColNameValue, String operator) {
		ArrayList<Integer> maxNumberIndexes = new ArrayList<Integer>();
		ArrayList<String> enteredeColNames = EnumtoString(htbColNameValue
				.keys());
		for (int i = 0; i < partitioned.size(); i++) {
			maxNumberIndexes.add(partitioned.get(i).howManyColIndexed(
					enteredeColNames));
		}
		LinkedList<ArrayList<Comparable>> result = new LinkedList<ArrayList<Comparable>>();
		ArrayList<Pair> foundPairs = new ArrayList<Pair>();
		int maxIndex = getMaxIndex(maxNumberIndexes);
		MultiDimension parMult = partitioned.get(maxIndex);
		ArrayList<String> hashing = parMult
				.searchAndGetHashedString(htbColNameValue);
		for (int i = 0; i < hashing.size(); i++) {
			LinkedList<Pair> myPairs = parMult.partitionedHashTable.get(hashing
					.get(i));
			for (int j = 0; j < myPairs.size(); j++) {
				Pair p = myPairs.removeFirst();
				Page page = (Page) (open(this.name + "," + p.pageNumber));
				LinkedList<ArrayList<Comparable>> records = page.records;

				for (int k = 0; k < records.size(); k++) {
					boolean[] flags = new boolean[htbColNameValue.size()];
					int f = 0;
					ArrayList<Comparable> col = records.removeFirst();
					Enumeration<String> keys = htbColNameValue.keys();
					while (keys.hasMoreElements()) {
						String s = keys.nextElement();
						Comparable value = htbColNameValue.get(s);
						int ind = this.index.get(s);
						Comparable comparedValue = col.get(ind);
						if (comparedValue.compareTo(value) == 0) {
							flags[f] = true;
						} else {
							flags[f] = false;
						}
						f++;
					}
					boolean allTrue;
					if (operator.equals("AND")) {
						allTrue = allTrueAnded(flags);
						if (allTrue) {
							result.addFirst(col);
							foundPairs.add(p);
						}
					} else {
						if (operator.equals("OR")) {
							allTrue = allTrueOred(flags);
							if (allTrue) {
								result.addFirst(col);
								foundPairs.add(p);
							}
						}
					}

					records.addLast(col);
				}

				myPairs.addLast(p);
			}

		}

		return foundPairs;
	}
	public LinkedList<ArrayList<Comparable>> search(
			Hashtable<String, String> htbColNameValue, String operator) {
		ArrayList<Integer> maxNumberIndexes = new ArrayList<Integer>();
		ArrayList<String> enteredeColNames = EnumtoString(htbColNameValue
				.keys());
		for (int i = 0; i < partitioned.size(); i++) {
			maxNumberIndexes.add(partitioned.get(i).howManyColIndexed(
					enteredeColNames));
		}
		LinkedList<ArrayList<Comparable>> result = new LinkedList<ArrayList<Comparable>>();
		ArrayList<Pair> foundPairs = new ArrayList<Pair>();
		int maxIndex = getMaxIndex(maxNumberIndexes);
		System.out.println(maxIndex);
		MultiDimension parMult = partitioned.get(maxIndex);
		System.out.println(htbColNameValue.size()+"before enter");
		ArrayList<String> hashing = parMult
				.searchAndGetHashedString(htbColNameValue);
		for(int k = 0 ;k<hashing.size();k++){
			System.out.print(hashing.get(k)+",");
		}
		System.out.println();
		for (int i = 0; i < hashing.size(); i++) {
			LinkedList<Pair> myPairs = parMult.partitionedHashTable.get(hashing
					.get(i));
			for (int j = 0; j < myPairs.size(); j++) {
				Pair p = myPairs.removeFirst();
				Page page = (Page) (open(this.name + "," + p.pageNumber));
				System.out.println(p.pageNumber+"page num");
				LinkedList<ArrayList<Comparable>> records = page.records;
				
				for (int k = 0; k < records.size(); k++) {
					
					boolean[] flags = new boolean[htbColNameValue.size()];
					int f = 0;
					ArrayList<Comparable> col = records.removeFirst();
					Enumeration<String> keys = htbColNameValue.keys();
					while (keys.hasMoreElements()) {
						String s = keys.nextElement();
						Comparable value = htbColNameValue.get(s);
						int ind = this.index.get(s);
						Comparable comparedValue = col.get(ind);
						if (comparedValue.compareTo(value) == 0) {
							System.out.println(true);
							flags[f] = true;
						} else {
							flags[f] = false;
						}
						f++;
					}
					boolean allTrue;
					if (operator.equals("AND")) {
						allTrue = allTrueAnded(flags);
						if (allTrue) {
							result.addFirst(col);
							foundPairs.add(p);
						}
					} else {
						if (operator.equals("OR")) {
							allTrue = allTrueOred(flags);
							if (allTrue) {
								result.addFirst(col);
								foundPairs.add(p);
							}
						}
					}

					records.addLast(col);
				}

				myPairs.addLast(p);
			}

		}

		return result;
	}

	public boolean allTrueAnded(boolean[] flags) {
		boolean flag = flags[0];
		for (int i = 1; i < flags.length; i++) {
			flag &= flags[i];
		}
		return flag;
	}

	public boolean allTrueOred(boolean[] flags) {
		boolean flag = flags[0];
		for (int i = 1; i < flags.length; i++) {
			flag |= flags[i];
		}
		return flag;
	}

	public LinkedList<ArrayList<Comparable>> linearSearch(
			Hashtable<String, String> htblColNameValue, String strOperator) {
		boolean found = false;

		Enumeration<String> tName = this.col.keys();
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
			return result;
		}
		for (int i = 0; i <= pagesSoFar; i++) {

			Page page = (Page) (open(this.name + "," + i));
			LinkedList<ArrayList<Comparable>> records = page.records;
			for (int k = 0; k < records.size(); k++) {
				boolean[] flags = new boolean[htblColNameValue.size()];
				int f = 0;
				ArrayList<Comparable> col = records.removeFirst();
				Enumeration<String> keys = htblColNameValue.keys();
				while (keys.hasMoreElements()) {
					String s = keys.nextElement();
					Comparable value = htblColNameValue.get(s);
					int ind = this.index.get(s);
					Comparable comparedValue = col.get(ind);
					if (comparedValue.compareTo(value) == 0) {
						flags[f] = true;
					} else {
						flags[f] = false;
					}
					f++;
				}
				Pair p = new Pair(i, k);
				boolean allTrue;
				if (strOperator.equals("AND")) {
					allTrue = allTrueAnded(flags);
					if (allTrue) {
						result.addFirst(col);
						foundPairs.add(p);
					}
				} else {
					if (strOperator.equals("OR")) {
						allTrue = allTrueOred(flags);
						if (allTrue) {
							result.addFirst(col);
							foundPairs.add(p);
						}
					}
				}

				records.addLast(col);
			}

		}

		return result;

	}

	public ArrayList<Pair> linearSearchAndGetPair(
			Hashtable<String, String> htblColNameValue, String strOperator) {
		boolean found = false;
		Enumeration<String> tName = this.col.keys();
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
			return foundPairs;
		}
		for (int i = 0; i <= pagesSoFar; i++) {

			Page page = (Page) (open(this.name + "," + i));
			LinkedList<ArrayList<Comparable>> records = page.records;
			for (int k = 0; k < records.size(); k++) {
				boolean[] flags = new boolean[htblColNameValue.size()];
				int f = 0;
				ArrayList<Comparable> col = records.removeFirst();
				Enumeration<String> keys = htblColNameValue.keys();
				while (keys.hasMoreElements()) {
					String s = keys.nextElement();
					Comparable value = htblColNameValue.get(s);
					int ind = this.index.get(s);
					Comparable comparedValue = col.get(ind);
					if (comparedValue.compareTo(value) == 0) {
						flags[f] = true;
					} else {
						flags[f] = false;
					}
					f++;
				}
				Pair p = new Pair(i, k);
				boolean allTrue;
				if (strOperator.equals("AND")) {
					allTrue = allTrueAnded(flags);
					if (allTrue) {
						result.addFirst(col);
						foundPairs.add(p);
					}
				} else {
					if (strOperator.equals("OR")) {
						allTrue = allTrueOred(flags);
						if (allTrue) {
							result.addFirst(col);
							foundPairs.add(p);
						}
					}
				}

				records.addLast(col);
			}

		}

		return foundPairs;

	}


	public Object open(String pageName) {
		//String pageName = this.name + "," + pageNum; 
		ObjectInputStream ois;
		Object p = null; 
		try {
			ois = new ObjectInputStream(new FileInputStream(new File(pageName + ".ser")));
			p = ois.readObject();
			ois.close();
		} catch (Exception e) { 
	
	}
		return p; 
	}
	public String toString() {
		String result = this.name; 
		for(int i = 0; i <=this.pagesSoFar ; i++) {
			Page p = (Page) open(this.name + "," + i); 
			result = result + "," + p.records.toString();
			
		}
		return result; 
	}
}
