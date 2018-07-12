LIB_PATH=/home/yaoliu/src_code/local/libthrift-1.0.0.jar:/home/yaoliu/src_code/local/slf4j-log4j12-1.5.8.jar:/home/yaoliu/src_code/local/slf4j-api-1.5.8.jar
all: clean
	mkdir bin bin/controller_classes bin/branch_classes
	javac -classpath $(LIB_PATH) -d bin/branch_classes/ src/DBranchHandler.java src/DistributedBranch.java src/LocalState.java src/ChannelState.java gen-java/* 
	javac -classpath $(LIB_PATH) -d bin/controller_classes/ src/Controller.java gen-java/* 


clean:
	rm -rf bin/

