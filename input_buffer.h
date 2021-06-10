//
// Created by yoehwan on 2021-06-07.
//

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>


typedef struct {
    char *buffer;
    size_t buffer_length;
    ssize_t input_length;
} InputBuffer;


InputBuffer *new_input_buffer() ;

void read_input(InputBuffer *input_buffer);