# THIS IS A COMMENT
# Run this with make utility in linux
# YOU CAN WRITE ANY linux TERMINAL COMMAND HERE AND IT WILL BE EXECUTED 
# THIS IS USED TO EXECUTE MULTIPLE TERMINAL COMMANDS WHEN ($make all) is entered in terminal (REMEMBER YOU HAVE TO BE IN THE DIRECTORY OF THIS # FILE TO BE ABLE TO RUN IT
# CUSTOMIZE THIS FILE TO UR NEEDS




JC = javac		#variable holding javac (java compiler)
CLASSPATH = ./classes/  #variable holding a path to your classes
TESTPATH = ./test/


#test: is called a target , you can think of it as a method that can be called in terminal using make utility ($make test)
test:                
		
		   $(JC) -d $(CLSSPATH) $(TESTPATH)*.java ; # same as javac -d "./classes/" 
# here test is being called when ever all is called , you can call "all" by typing in terminal ($make all) which is going to call ($make test)
all: test 

# clean is the same (here it calls rm which is used to remove)
clean:	 
	   rm $(CLSSPATH)*                    #same as rm ./classes/* (* is used to refer to all files in that folder)
