#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>


typedef struct {
    char *buffer;
    size_t buffer_length;
    ssize_t input_length;
} InputBuffer;


InputBuffer *new_input_buffer() {
    InputBuffer *input_buffer = malloc(sizeof(InputBuffer));
    input_buffer->buffer = NULL;
    input_buffer->buffer_length = 0;
    input_buffer->input_length = 0;
    return input_buffer;
}

void close_input_buffer(InputBuffer *input_buffer) {
    free(input_buffer->buffer);
    free(input_buffer);
}


void print_init_prompt(){
    printf("Custom sqlite version 1.0.0\n");
}
void print_prompt() {
    printf("sqlite > ");
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


int main() {
    print_init_prompt();
    InputBuffer *input_buffer = new_input_buffer();

    while (true) {
        print_prompt();
        read_input(input_buffer);

        if (strcmp(input_buffer->buffer, ".exit") == 0) {
            close_input_buffer(input_buffer);
            exit(EXIT_SUCCESS);
        } else {
            printf("Unrecognized command '%s' .\n", input_buffer->buffer);
        }
    }

}




