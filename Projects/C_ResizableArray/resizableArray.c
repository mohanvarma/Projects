#include "resizableArray.h"

// This implementation makes sure that the array
// is always between 25% and 100% full

void resize(resizableArray* array, int size)
{
	void *fullArray = array->array;
	void *newArray = realloc(fullArray, sizeof(int)*size);
	
	// Malloc error
	if(!newArray)
	{
		printf("resize: Malloc error\n");
		return;
	}

	array->array = newArray;
	array->capacity = size;
}

/* Initialize Array with a default occupancy:1 */
void initializeArray(resizableArray* array)
{
	array->occupancy = 0;
	array->capacity = 1;
	// Assume int
	array->array = malloc(sizeof(int)*(array->capacity));

	// malloc error
	if(!array->array)
		printf("initializeArray: Malloc error\n");
}

/* Insert an element
 * while doing this, array is automatically adjusted
 */
void insertElement(resizableArray* array, int element)
{
	if(array->occupancy == array->capacity)
	{
		// Double the capacity if it's full
		resize(array, 2*(array->capacity));

		// Malloc error
		if(!array->array)
		{
			printf("insert: Malloc error\n");
			return;
		}
	}

	((int*)array->array)[array->occupancy++] = element;
}

/* Deletes the last element
 * While doing this, array is automatically adjusted
 */
int removeElement(resizableArray* array)
{
	// Already empty
	if(array->occupancy == 0)
	{
		printf("remove: nothing to remove\n");
		return -1;
	}

	int item = ((int*)array->array)[--array->occupancy];

	if(array->occupancy > 0 && array->occupancy == array->capacity/4)
	{
		resize(array, array->capacity/2);

		// Malloc error
		if(!array->array)
		{
			printf("remove: Malloc error\n");
			return -1;
		}
	}

	return item;
}

/* Free array */
void freeArray(resizableArray *array)
{
	free(array->array);
	array->array = NULL;
	array->occupancy = 0;
	array->capacity = 0;
}
