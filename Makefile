main: sort.cpp sort.h
	g++ sort.cpp -std=c++11 -o main
	
clean:
	rm -rf main