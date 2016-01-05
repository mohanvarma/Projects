#include "resizableArray.h"

// Also verified with valgrind ./a.out --tool=mem-check
// No unfreed memory

int main()
{
	resizableArray array;
	array.array = NULL;

	initializeArray(&array);
	printf("Array current size: %d\n", array.occupancy);
	printf("Array capacity: %d\n", array.capacity);
	printf("Array pointer: %p\n", array.array);

	insertElement(&array, 10);
	printf("Array current size: %d\n", array.occupancy);
	printf("Array capacity: %d\n", array.capacity);
	printf("Array pointer: %p\n", array.array);

	int pop = removeElement(&array);
	printf("Removed element: %d\n", pop);
	printf("Array current size: %d\n", array.occupancy);
	printf("Array capacity: %d\n", array.capacity);
	printf("Array pointer: %p\n", array.array);

	freeArray(&array);
	printf("Array pointer: %p\n", array.array);

	initializeArray(&array);
	printf("Array current size: %d\n", array.occupancy);
	printf("Array capacity: %d\n", array.capacity);
	printf("Array pointer: %p\n", array.array);

	insertElement(&array, 10);
	printf("Array current size: %d\n", array.occupancy);
	printf("Array capacity: %d\n", array.capacity);
	printf("Array pointer: %p\n", array.array);
	
	insertElement(&array, 20);
	printf("Array current size: %d\n", array.occupancy);
	printf("Array capacity: %d\n", array.capacity);
	printf("Array pointer: %p\n", array.array);

	insertElement(&array, 30);
	printf("Array current size: %d\n", array.occupancy);
	printf("Array capacity: %d\n", array.capacity);
	printf("Array pointer: %p\n", array.array);

	insertElement(&array, 40);
	printf("Array current size: %d\n", array.occupancy);
	printf("Array capacity: %d\n", array.capacity);
	printf("Array pointer: %p\n", array.array);

	insertElement(&array, 50);
	printf("Array current size: %d\n", array.occupancy);
	printf("Array capacity: %d\n", array.capacity);
	printf("Array pointer: %p\n", array.array);

	int counter = 0;
	for(counter = 0; counter < 5; counter++)
	{
		int pop = removeElement(&array);
		printf("Removed element: %d\n", pop);
		printf("Array current size: %d\n", array.occupancy);
		printf("Array capacity: %d\n", array.capacity);
		printf("Array pointer: %p\n", array.array);
	}

	freeArray(&array);
	printf("Array pointer: %p\n", array.array);


	// Unit tests
	initializeArray(&array);
	for(counter = 0; counter < 16; counter++)
	{
		insertElement(&array, counter);
	}

	if(array.occupancy != 16)
	{
		printf("Error: current size is wrong\n");
		return -1;
	}

	if(array.capacity != 16)
	{
		printf("Error: capacity is wrong\n");
		return -1;
	}

	insertElement(&array, counter);

	if(array.occupancy != 17)
	{
		printf("Error: current size is wrong\n");
		return -1;
	}


	if(array.capacity != 32)
	{
		printf("Error: capacity is wrong\n");
		return -1;
	}

	for(counter = 0; counter < 9; counter++)
	{
		pop = removeElement(&array);
	}

	if(pop != 8)
	{
		printf("Error: removeElement failed\n");
	}


	if(array.occupancy != 8 || array.capacity != 16)
	{
		printf("Error: Auto resizing failed\n");
	}

	freeArray(&array);

	if(array.occupancy != 0 || array.capacity != 0 || array.array != NULL)
	{
		printf("Error: Free failed\n");
	}

	printf("Success: All tests passed\n");

	return 0;
}
