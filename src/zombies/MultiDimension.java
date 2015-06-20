package zombies;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.LinkedList;


public class MultiDimension implements Serializable{
	
	Hashtable<String,LinkedList<Pair>> partitionedHashTable = new Hashtable<String,LinkedList<Pair>>();
	ArrayList<Hashtable <Comparable,String>> hashtables = new ArrayList<>();
	Hashtable<String,String> htblColNames;
	ArrayList<Integer> hashedIndex = new ArrayList<Integer>();
	final int partitions = 16;
	//int size;
	public MultiDimension(Hashtable<String,String> htblColNames){
		//this.size = (int) Math.pow(2,htblColNames.size()+1);
		this.htblColNames = htblColNames;
		// No duplicates so no two values with the same hash
		/*for(int i = 0 ;i<size;i++){
			Integer k = new Integer(i);
			String key =Integer.toBinaryString(k);
			partitionedHashTable.put(key,new LinkedList<Pair>());
		}*/ 
		for(int i = 0 ; i<this.htblColNames.size(); i++){
			hashtables.add(i,new Hashtable<Comparable,String>()) ;
		}
		
		
		
	}
	public int getHashedColIndexInHashTable(String s ){
		ArrayList<String> t= EnumtoString(this.htblColNames.keys());
		return t.indexOf(s);
	}
	public static ArrayList<String> EnumtoString(Enumeration<String> e){
		ArrayList<String> r = new ArrayList<String>();
		while(e.hasMoreElements()){
			r.add(e.nextElement());
		}
		return r;
	}
	public boolean found(ArrayList<String> colNames){
		Enumeration<String> keys = htblColNames.keys();
		ArrayList<String> f = EnumtoString(keys);
		if(colNames.size()==f.size()){
			if(f.containsAll(colNames)){
				return true;
			}
		}
		return false;
	}
	public boolean equalityDoNotCare(String s ,String t){
		return (s.equals(t.substring(0,s.length())));
	}
	public void removePair(Pair remove) {
		Enumeration<String> keys = partitionedHashTable.keys();
		while(keys.hasMoreElements()) {
			String hashed = keys.nextElement();
			LinkedList toDelete = partitionedHashTable.get(hashed);
			for(int i = 0; i<toDelete.size();i++) {
				Pair x = (Pair)toDelete.removeFirst();
				if(!(x.equaity(remove))) 
					toDelete.addLast(x);		
			}
		}
	}
	public void insert(int page , int row , String key ){

		if(partitionedHashTable.containsKey(key)){
			partitionedHashTable.get(key).addLast(new Pair(page,row));
		}else{
			partitionedHashTable.put(key, new LinkedList<Pair>());
			LinkedList<Pair> n = partitionedHashTable.get(key);
			n.addLast(new Pair(page,row));			
		}
	}
	public void find(String key){
		LinkedList list = partitionedHashTable.get(key);
	}
	
	public void createMultiDimension(Hashtable<String,String> htblColNames){
		int size = htblColNames.size();
		
	}
	public String linearHash(Comparable data ){
		if(data == null){
			return "0000" ;
		}
		int x = partitions;
		
		int n = data.hashCode() &(x-1);	
		while(n>=x){
			x = x/2;
			n =data.hashCode() &(x-1);
		}
		Integer num = new Integer(n);
		String ret = Integer.toBinaryString(num);
		while(ret.length()<4){
			ret=""+0+ret;
		}
	
		return ret;
		
	}
	public int howManyColIndexed(ArrayList<String> enteredColNames){
		ArrayList<String> myColNames = EnumtoString(htblColNames.keys());
		int c = 0 ;
		for(int i = 0 ;i<enteredColNames.size();i++){
			if(myColNames.contains(enteredColNames.get(i))){
				c++;
			}
		}
		return c;
	}
	
	public ArrayList<String> searchAndGetHashedString(Hashtable<String,String> htbColNameValue){
		System.out.println(htbColNameValue.size()+"Length");
		ArrayList<String> columnNames = EnumtoString(htbColNameValue.keys());
		ArrayList<String> myColumnNames = EnumtoString(htblColNames.keys());
		String hashed[] = new String[htblColNames.size()];
		//String hashed[] = new String[htbColNameValue.size()];
		System.out.println(hashed.length+"Length");
		int k = 0 ;
		boolean flag = false;
		
		for (int i  = 0 ;i<columnNames.size();i++){
			for (int j = 0 ;j<myColumnNames.size()&&!flag;j++ ){
				System.out.println(myColumnNames.get(j));
				if(columnNames.get(i).equals(myColumnNames.get(j))){
					k = j;
					flag = true;
					System.out.println(htbColNameValue.get(columnNames.get(i))+"Hashing...");
					hashed[k] = linearHash(htbColNameValue.get(columnNames.get(i)));
				}
			}
			flag = false;
		}
		
		//"0000020010" from 0 to 4 0000 - from 8 to 12 0010
		for(int l = 0 ;l<hashed.length;l++){
			if(hashed[l]!=null){
				System.out.print(hashed[l]+",");
			}else{
				System.out.print("NULL"+",");
			}	
		}
		System.out.println();
		String searchWithDonotCares = "";
		for(int i = 0 ;i<hashed.length;i++){
			if(hashed[i] != null){
				searchWithDonotCares+=i+""+hashed[i];
			}
		}
		System.out.println(searchWithDonotCares+"cccc");
		ArrayList <String> searchFor = EnumtoString(partitionedHashTable.keys());
		for(int i = 0 ;i<searchFor.size();i++){
			System.out.print(searchFor.get(i)+"-");
			
		}
		System.out.println();
		for(int i = 0 ;i<hashed.length;i++){
			if(hashed[i]!=null)
				searchFor = stringEqual(""+i+""+hashed[i],searchFor);
		}
		System.out.println(searchFor.size());
		for(int i = 0 ;i<searchFor.size();i++){
			System.out.println(searchFor.get(i)+"asd");
		}
		return searchFor;
	}
	public ArrayList<String> stringEqual(String s , ArrayList<String> t){
		ArrayList<String> r = new ArrayList<String>();
		int start = 4*Integer.parseInt(""+s.charAt(0));
		if(start!=0){
			start--;
		}
		if(s == null){
			return t;
		}
		System.out.println(start+" start");
		s=s.substring(1);
		for(int i = 0 ;i<t.size();i++){
			System.out.println(s+"nn");
			System.out.println(t.get(i).substring(start,start+4)+"substring");
			if(t.get(i).substring(start,start+4).equals(s)){
				r.add(t.get(i));
			}
		}
		System.out.println(r.size()+" The size");
		return r;
	}
	public String toString() {
		String result = ""; 
		Enumeration<String> key = this.partitionedHashTable.keys();

		while(key.hasMoreElements()) {
			String temp = key.nextElement(); 
			result = result + temp  +":" +  this.partitionedHashTable.get(temp) + ";"; 
		}
		return result; 
	}
}