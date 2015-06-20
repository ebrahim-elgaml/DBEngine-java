package zombies;

public class Pair{
		int pageNumber;
		int rowNumber;
		public Pair(int x , int y ){
			pageNumber = x;
			rowNumber  = y;
		}
		boolean equaity(Pair p ){
			return (pageNumber==p.pageNumber && rowNumber==p.rowNumber);
		}
		public String toString() {
			//System.out.println("da5alt hena");
			return this.pageNumber + "," + this.rowNumber ; 
		}
	}