#include <stdio.h>
#include <malloc.h>
#include <stdlib.h>

typedef struct {
	void *array;
	int capacity;
	int occupancy;
} resizableArray;

/* Initialize Array with a default size */
void initializeArray(resizableArray* array);

/* Insert an element of a type
 * while doing this, array is automatically adjusted
 */
void insertIntoArray(resizableArray* array, void* element, int type);

/* Free array */
void freeArray(resizableArray *array);
