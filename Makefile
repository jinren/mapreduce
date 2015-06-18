CXX	:= gcc
CXXFLAGS:= -g -pthread -Wall
OBJECTS	:= mapreduce.o
mapreduce: $(OBJECTS)
	$(CXX) $(OBJECTS) -lpthread -o mapreduce

mapreduce.o: mapreduce.c
	$(CXX) $(INCLUDE) $(CXXFLAGS) -c mapreduce.c -o mapreduce.o

clean:
	rm *.o mapreduce
