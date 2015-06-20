package zombies;
import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;


public class Page implements Serializable {
	int tuples;
	LinkedList<ArrayList<Comparable>> records;
	ArrayList<Boolean> tombStone;
	//String tableName; 
	public Page() {
		tuples = 0;
		tombStone = new ArrayList<Boolean>();
		records = new LinkedList<ArrayList<Comparable>>();
		 
	}
	
	public boolean insert(LinkedList<Comparable> myTuple){
		/*if(tuples < 20) {
			records.addLast(myTuple);;
			tuples++;
			return true;
		}
		else{
			return false;
		}*/
		return true; 
	}
	public boolean delete(Comparable Key) {
		/*Tuple myTuple = this.search(Key);
		int index = records.indexOf(myTuple);
		
		if(index != -1 && (!tombStone.get(index))) {
			tombStone.add(index, true);
			tuples--;
			return true;
		}
		else {
			System.out.println("Record not found");
			return false;
		}
	}*/
		return true; }
	// check 2alabtaha
	public ArrayList<ArrayList<Comparable>> search (Object Key) {
		 /*ArrayList<ArrayList<Comparable>> wanted = null;
		 for(int i = 0; i<tuples; i++) {
			 for(int j = 0; j<records.get(i).size() ; j++){
				 if(Key.equals(records.get(i).get(j))) {
					 wanted.add(records.get(i));
				 }
			 }
		 }*/
		 return null;
	}
	public String toString() {
		String result ="";
		for(int i = 0; i < this.records.size(); i++) {
			for(int j = 0; j < records.getFirst().size(); j++) 
				result = result + "," + records.getFirst().get(j); 
		}
		return result; 
	}

	
	
}
