

#include "meta_data.h"

#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)


#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255

typedef struct {
    u_int32_t id;
    char username[COLUMN_USERNAME_SIZE];
    char email[COLUMN_EMAIL_SIZE];
} Row;

const u_int32_t ID_SIZE = size_of_attribute(Row, id);
const u_int32_t USERNAME_SIZE = size_of_attribute(Row, username);
const u_int32_t EMAIL_SIZE = size_of_attribute(Row, email);
const u_int32_t ID_OFFSET = 0;
const u_int32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const u_int32_t EMAIL_OFFSET =  USERNAME_OFFSET + USERNAME_SIZE;
const u_int32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;


#define TABLE_MAX_PAGES 100

///depends on system byte 4kilobytes => 32bits system
const u_int32_t PAGE_SIZE = 4096;
const u_int32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const u_int32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;
typedef struct {
    u_int32_t num_rows;
    void *pages[TABLE_MAX_PAGES];
} Table;

Table *new_table(){
    Table *table = malloc(sizeof(Table));
    table->num_rows=0;
    for(u_int32_t i = 0; i<TABLE_MAX_PAGES;i++){
        table->pages[i]=NULL;
    }
    return  table;
}

void free_table(Table *table){
    for(int i=0;table->pages[i];i++){
        free(table->pages[i]);
    }
    free(table);
}





typedef enum {
    PREPARE_SUCCESS,
    PREPARE_UNRECOGNIZED_STATEMENT,
    PREPARE_SYNTAX_ERROR,
} PrepareResult;

typedef enum {
    STATEMENT_INSERT,
    STATEMENT_SELECT,
} StatementType;

typedef struct {
    StatementType type;
    Row row_to_insert;
} Statement;


typedef enum {
    EXECUTE_TABLE_FULL,
    EXECUTE_SUCCESS,
}ExecuteResult;

void print_init_prompt() {
    printf("Custom sqlite version 1.0.0\n");
}

void print_prompt() {
    printf("sqlite > ");
}

PrepareResult prepare_statement(InputBuffer *input_buffer, Statement *statement) {
    if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
        statement->type = STATEMENT_INSERT;
        int args_assigned = sscanf(input_buffer->buffer,
                                   "insert %d %s %s",
                                   &(statement->row_to_insert.id),
                                   statement->row_to_insert.username,
                                   statement->row_to_insert.email);
        if (args_assigned < 3) {
            return PREPARE_SYNTAX_ERROR;
        }
        return PREPARE_SUCCESS;
    }
    if (strcmp(input_buffer->buffer, "select") == 0) {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }
    return PREPARE_UNRECOGNIZED_STATEMENT;
}

void serialize_row(Row *source, void *destination) {
    memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
    memcpy(destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
    memcpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

void deserialize_row(void *source, Row *destination) {
    memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
    memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}


void print_row(Row *row){
    printf("(%d, %s, %s)\n",row->id,row->username,row->email);
}

void *row_slot(Table *table, u_int32_t row_num) {
    u_int32_t page_num = row_num / ROWS_PER_PAGE;
    void *page = table->pages[page_num];
    if (page == NULL) {
        // Allocate memory only when we try to access page
        page = table->pages[page_num] = malloc(PAGE_SIZE);
    }
    u_int32_t row_offset = row_num % ROWS_PER_PAGE;
    u_int32_t byte_offset = row_offset * ROW_SIZE;
    return page + byte_offset;
}

ExecuteResult execute_insert(Statement *statement,Table *table){
    if(table->num_rows>=TABLE_MAX_ROWS){
        return  EXECUTE_TABLE_FULL;
    }
    Row *row_to_insert = &(statement->row_to_insert);
    serialize_row(row_to_insert, row_slot(table,table->num_rows));
    table->num_rows+=1;
    return EXECUTE_SUCCESS;

}



ExecuteResult execute_select(Statement *statement,Table *table){
    Row row;
    for(u_int32_t i =0;i<table->num_rows;i++){
        deserialize_row(row_slot(table,i),&row);
        print_row(&row);
    }
    return  EXECUTE_SUCCESS;
}


ExecuteResult execute_statement(Statement *statement,Table *table) {
    switch (statement->type) {
        case STATEMENT_INSERT:
            return execute_insert(statement,table);
        case STATEMENT_SELECT:
            return execute_select(statement,table);
    }
}

int main() {
    print_init_prompt();
    InputBuffer *input_buffer = new_input_buffer();
    Table *table = new_table();
    while (true) {
        print_prompt();
        read_input(input_buffer);

        //check_meta_data_command start with '.' .
        check_meta_data_command(input_buffer);

        Statement statement;
        switch (prepare_statement(input_buffer, &statement)) {
            case PREPARE_SUCCESS:
                break;;
            case PREPARE_UNRECOGNIZED_STATEMENT:
                printf("Unrecognized keyword at start of '%s' . \n",
                       input_buffer->buffer);
                continue;
        }
        execute_statement(&statement,table);
        printf("Executed.\n");
    }

}




