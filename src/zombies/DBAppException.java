package zombies;

public class DBAppException extends Exception{

      //Parameterless Constructor
      public DBAppException() {}

      //Constructor that accepts a message
      public DBAppException(String message)
      {
         super(message);
      }
 }