#include <stdio.h>
#include <malloc.h>
#include <stdlib.h>

typedef struct {
	void *array;
	int capacity;
	int occupancy;
} resizableArray;

/* Initialize Array with a default size:1 */
void initializeArray(resizableArray* array);

/* Insert an element
 * while doing this, array is automatically adjusted
 */
void insertElement(resizableArray* array, int element);

/* Deletes the last element
 * While doing this, array is automatically adjusted
 */
int removeElement(resizableArray* array);

/* Free array */
void freeArray(resizableArray *array);
