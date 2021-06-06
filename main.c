#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>


typedef struct {
    char *buffer;
    size_t buffer_length;
    ssize_t input_length;
} InputBuffer;

typedef enum {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND,
} MetaCommandResult;

typedef enum {
    PREPARE_SUCCESS,
    PREPARE_UNRECOGNIZED_STATEMENT,
} PrepareResult;

typedef enum {
    STATEMENT_INSERT,
    STATEMENT_SELECT,
} StatementType;

typedef struct {
    StatementType type;
} Statement;

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


void print_init_prompt() {
    printf("Custom sqlite version 1.0.0\n");
}

void print_prompt() {
    printf("sqlite > ");
}

PrepareResult prepare_statement(InputBuffer *input_buffer, Statement *statement) {
    if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
        statement->type = STATEMENT_INSERT;
        return PREPARE_SUCCESS;
    }
    if (strcmp(input_buffer->buffer, "select") == 0) {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }
    return PREPARE_UNRECOGNIZED_STATEMENT;
}

void execute_statement(Statement *statement) {
    switch (statement->type) {
        case STATEMENT_INSERT:
            printf("Insert!\n");
            break;
        case STATEMENT_SELECT:
            printf("Select!\n");
            break;
    }
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

/// Non-SQL statements like .exit are called “meta-commands”.
/// They all start with a dot,
/// so we check for them and handle them in a separate function.
MetaCommandResult do_meta_command(InputBuffer *input_buffer) {
    if (strcmp(input_buffer->buffer, ".exit") == 0) {
        close_input_buffer(input_buffer);
        exit(EXIT_SUCCESS);
    } else {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

int main() {
    print_init_prompt();
    InputBuffer *input_buffer = new_input_buffer();

    while (true) {
        print_prompt();
        read_input(input_buffer);

        //meta_command start with '.' .
        if (input_buffer->buffer[0] == '.') {
            switch (do_meta_command(input_buffer)) {
                case META_COMMAND_SUCCESS:
                    continue;
                case META_COMMAND_UNRECOGNIZED_COMMAND:
                    printf("Unrecognized Command [%s] .\n", input_buffer->buffer);
                    continue;
            };
        }
        Statement statement;
        switch (prepare_statement(input_buffer, &statement)) {
            case PREPARE_SUCCESS:
                break;;
            case PREPARE_UNRECOGNIZED_STATEMENT:
                printf("Unrecognized keyword at start of '%s' . \n", input_buffer->buffer);
                continue;
        }
        execute_statement(&statement);
        printf("Executed.\n");
    }

}




