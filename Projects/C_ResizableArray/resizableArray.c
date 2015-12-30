#include "resizableArray.h"

/* Initialize Array with a default size */
void initializeArray(resizableArray* array, int type)
{
	// Default capacity is 10
	array->capacity = 10;
	array->occupancy = 0;
	
	// Char
	if (type == 0)
	{
		array->array = malloc(10*sizeof(char));
	}
	else if (type == 1)
	{
		array->array = malloc(10*sizeof(int));
	}
}

/* Insert an element of a type
 * while doing this, array is automatically adjusted
 */
void insertIntoArray(resizableArray* array, void* element, int type)
{
	if (!array)
		return;

	if(array->occupancy <= array->capacity)
	{
		array->array[array->occupancy++] = element;
		return;
	}
	else
	{
		array->capacity = (array->capacity)*2;
		array->array = realloc(array->array, (array->capacity) * );
	}
}


/* Free array */
void freeArray(resizableArray *array);
