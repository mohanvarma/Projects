all: clean resizableArrayLib main 

clean:
	rm -rf *.so

resizableArrayLib: resizableArray.c resizableArray.h
	gcc -Wall -fPIC -g -ggdb -c resizableArray.c -o libresizableArray.so

main: main.c resizableArray.h
	gcc main.c -L. -lresizableArray

test:
	output = $(shell valgrind ./a.out --tool=mem-check)
	echo $(output)

