//
// Created by yoehwan on 2021-06-07.
//

#include "input_buffer.h"

InputBuffer *new_input_buffer() {
    InputBuffer *input_buffer = malloc(sizeof(InputBuffer));
    input_buffer->buffer = NULL;
    input_buffer->buffer_length = 0;
    input_buffer->input_length = 0;
    return input_buffer;
}

void read_input(InputBuffer *input_buffer) {
    ssize_t byte_read = getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);
    if (byte_read <= 0) {
        printf("Error reading input\n");
        exit(EXIT_FAILURE);
    }
    input_buffer->input_length = byte_read - 1;
    input_buffer->buffer[byte_read - 1] = 0;
}