hash_grouping_update : main.o MyHash.o  KVBuffer.o
	g++ -o hash_grouping_update main.o MyHash.o KVBuffer.o

main.o : MyHash.h timer.h 
	g++ -c main.cpp
MyHash.o : MyHash.h murmurhash.h
	g++ -c MyHash.cpp
KVBuffer.o : KVBuffer.h
	g++ -c KVBuffer.cpp
clean :
	rm hash_grouping_update main.o MyHash.o  KVBuffer.o
