package zombies;

public class DBEngineException extends Exception {
	public DBEngineException() {
		
	}

    //Constructor that accepts a message
    public DBEngineException(String message)
    {
       super(message);
    }

}
