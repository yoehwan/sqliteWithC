//
// Created by yoehwan on 2021-06-07.
//

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

#ifndef SQLITE_IO_CONTROLL_H
#define SQLITE_IO_CONTROLL_H

#endif //SQLITE_IO_CONTROLL_H


typedef struct {
    char *buffer;
    size_t buffer_length;
    ssize_t input_length;
} InputBuffer;


InputBuffer *new_input_buffer() ;

void read_input(InputBuffer *input_buffer);