//
// Created by yoehwan on 2021-06-07.
//

#include "meta_data.h"


void close_input_buffer(InputBuffer *input_buffer) {
    free(input_buffer->buffer);
    free(input_buffer);
}

/// Non-SQL statements like .exit are called “meta-commands”.
/// They all start with a dot,
/// so we check for them and handle them in a separate function.
void check_meta_data_command(InputBuffer *input_buffer) {
    if (input_buffer->buffer[0] != '.')return;
    if (strcmp(input_buffer->buffer, ".exit")==0) {
        close_input_buffer(input_buffer);
        exit(EXIT_SUCCESS);
    }
    printf("Unrecognized Command [%s] .\n", input_buffer->buffer);
}