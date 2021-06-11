

#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include "meta_data.h"
#include "const.h"

typedef struct {
    int file_descriptor;
    u_int32_t file_length;
    void *pages[TABLE_MAX_PAGES];
} Pager;


typedef struct {
    u_int32_t num_rows;
    Pager *pager
} Table;


Pager *pager_open(const char *filename) {
    int fd = open(filename,
                  O_RDWR | // Read/Write Mode
                  O_CREAT,       // Create file if it does not exist
                  S_IWUSR |      // User write permission
                  S_IRUSR        // User read permission
    );
    if (fd == -1) {
        printf("Unable to open file\n");
        exit(EXIT_FAILURE);
    }
    off_t file_length = lseek(fd, 0, SEEK_END);
    Pager *pager = malloc(sizeof(Pager));
    pager->file_descriptor = fd;
    pager->file_length = file_length;
    for (u_int32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        pager->pages[i] = NULL;
    }
    return pager;
}

void pager_flush(Pager *pager,u_int32_t page_num,u_int32_t size){
    if(pager->pages[page_num]==NULL){
        printf("Trued to flush null page\n");
        exit(EXIT_FAILURE);
    }
    off_t offset = lseek(pager->file_descriptor,page_num*PAGE_SIZE,SEEK_SET);

    if(offset==-1){
        printf("Error seeking: %d\n",errno);
        exit(EXIT_FAILURE);
    }
    ssize_t byte_written = write(pager->file_descriptor,pager->pages[page_num],size);
    if(byte_written==-1){
        printf("Error writing: %d\n",errno);
    }
    exit(EXIT_FAILURE);
}

void *db_close(Table *table){
    Pager  *pager =table->pager;
    u_int32_t num_full_pages = table->num_rows/ROWS_PER_PAGE;

    for(u_int32_t i = 0;i<num_full_pages;i++){
        if(pager->pages[i]==NULL){
            continue;
        }
        pager_flush(pager,i,PAGE_SIZE);
        free(pager->pages[i]);
        pager->pages[i]=NULL;
    }

    u_int32_t num_additional_rows = table->num_rows%ROWS_PER_PAGE;
    if(num_additional_rows>0){
        u_int32_t page_num = num_full_pages;
        if(pager->pages[page_num]!=NULL){
            pager_flush(pager,page_num,num_additional_rows*ROW_SIZE);
            free(pager->pages[page_num]);
            pager->pages[page_num]=NULL;
        }
    }
    int result = close(pager->file_descriptor);
    if(result==-1){
        printf("Error closing db file.\n");
        exit(EXIT_FAILURE);
    }
    for(u_int32_t i=0;i<TABLE_MAX_PAGES;i++){
        void *page = pager->pages[i];
        if(page){
            free(page);
            pager->pages[i]=NULL;
        }
    }
    free(pager);
    free(table);
}
Table *db_open(const char *filename) {
    Pager *pager = pager_open(filename);
    u_int32_t num_rows = pager->file_length / ROW_SIZE;
    Table *table = malloc(sizeof(Table));
    table->pager = pager;
    table->num_rows = num_rows;
    return table;
}



typedef enum {
    PREPARE_SUCCESS,
    PREPARE_NEGATIVE_ID,
    PREPARE_STRING_TOO_LONG,
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
} ExecuteResult;

typedef struct {
    Table *table;
    u_int32_t row_num;
    bool end_of_table;
}Cursor;

Cursor *table_start(Table *table){
    Cursor *cursor = malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->row_num=0;
    cursor->end_of_table=(table->num_rows==0);
}

Cursor *table_end(Table *table){
    Cursor *cursor = malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->row_num = table->num_rows;
    cursor->end_of_table = true;
    return  cursor;
}

void print_init_prompt() {
    printf("Custom sqlite version 1.0.0\n");
}

void print_prompt() {
    printf("sqlite > ");
}

PrepareResult prepare_insert(InputBuffer *input_buffer, Statement *statement) {
    statement->type = STATEMENT_INSERT;
    char *keyword = strtok(input_buffer->buffer, " ");
    char *id_string = strtok(NULL, " ");
    char *username = strtok(NULL, " ");
    char *email = strtok(NULL, " ");
    if (id_string == NULL || username == NULL || email == NULL) {
        return PREPARE_SYNTAX_ERROR;
    }
    int id = atoi(id_string);
    if (id < 0) {
        return PREPARE_NEGATIVE_ID;
    }
    if (strlen(username) > COLUMN_USERNAME_SIZE) {
        return PREPARE_STRING_TOO_LONG;
    }
    if (strlen(email) > COLUMN_EMAIL_SIZE) {
        return PREPARE_STRING_TOO_LONG;
    }
    statement->row_to_insert.id = id;
    strcpy(statement->row_to_insert.email, email);
    strcpy(statement->row_to_insert.username, username);
    return PREPARE_SUCCESS;
}

PrepareResult prepare_statement(InputBuffer *input_buffer, Statement *statement) {
    if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
        return prepare_insert(input_buffer, statement);
    }
    if (strcmp(input_buffer->buffer, "select") == 0) {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }
    return PREPARE_UNRECOGNIZED_STATEMENT;
}


void serialize_row(Row *source, void *destination) {
    memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
//    memcpy(destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
//    memcpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
    strncpy(destination+USERNAME_OFFSET,source->username,USERNAME_SIZE);
    strncpy(destination+EMAIL_OFFSET,source->email,EMAIL_SIZE);

}

void deserialize_row(void *source, Row *destination) {
    memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
    memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}


void print_row(Row *row) {
    printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

void *get_page(Pager *pager, u_int32_t page_num) {
    if (page_num > TABLE_MAX_PAGES) {
        printf("Tried to fetch page numver out of bounds. %d > %d\n", page_num, TABLE_MAX_PAGES);
        exit(EXIT_FAILURE);
    }
    if(pager->pages[page_num]==NULL){
        void *page = malloc(PAGE_SIZE);
        u_int32_t num_pages = pager->file_length/PAGE_SIZE;
        if(pager->file_length%PAGE_SIZE){
            num_pages+=1;
        }
        if(page_num<=num_pages){
            lseek(pager->file_descriptor,page_num*PAGE_SIZE,SEEK_SET);
            ssize_t bytes_read = read(pager->file_descriptor,page,PAGE_SIZE);
            if(bytes_read==-1){
                printf("Error reading file: %d\n",errno);
                exit(EXIT_FAILURE);
            }
        }
        pager->pages[page_num] = page;
    }
    return pager->pages[page_num];
}

void *cursor_value(Cursor *cursor) {
    u_int32_t row_num = cursor->row_num;
    u_int32_t page_num = row_num / ROWS_PER_PAGE;
    void *page = get_page(cursor->table->pager, page_num);
    u_int32_t row_offset = row_num % ROWS_PER_PAGE;
    u_int32_t byte_offset = row_offset * ROW_SIZE;
    return page + byte_offset;
}

void cursor_advance(Cursor *cursor){
    cursor->row_num+=1;
    if(cursor->row_num>=cursor->table->num_rows){
        cursor->end_of_table=true;
    }
}

ExecuteResult execute_insert(Statement *statement, Table *table) {
    if (table->num_rows >= TABLE_MAX_ROWS) {
        return EXECUTE_TABLE_FULL;
    }
    Row *row_to_insert = &(statement->row_to_insert);
    Cursor *cursor = table_end(table);
    serialize_row(row_to_insert, cursor_value(cursor));
    table->num_rows += 1;
    return EXECUTE_SUCCESS;
}


ExecuteResult execute_select(Statement *statement, Table *table) {
    Cursor *cursor  = table_start(table);
    Row row;
    while (!(cursor->end_of_table)){
        deserialize_row(cursor_value(cursor), &row);
        print_row(&row);
        cursor_advance(cursor);
    }
    free(cursor);
    return EXECUTE_SUCCESS;
}


ExecuteResult execute_statement(Statement *statement, Table *table) {
    switch (statement->type) {
        case STATEMENT_INSERT:
            return execute_insert(statement, table);
        case STATEMENT_SELECT:
            return execute_select(statement, table);
    }
}
void close_input_buffer(InputBuffer *input_buffer) {
    free(input_buffer->buffer);
    free(input_buffer);
}

/// Non-SQL statements like .exit are called “meta-commands”.
/// They all start with a dot,
/// so we check for them and handle them in a separate function.
void check_meta_data_command(InputBuffer *input_buffer,Table *table) {
    if (strcmp(input_buffer->buffer, ".exit")==0) {
        db_close(table);
        close_input_buffer(input_buffer);
        exit(EXIT_SUCCESS);
    }
//    else{
//        return;
//        printf("Unrecognized Command [%s] .\n", input_buffer->buffer);
//    }
}

int main(int argc,char *argv[]) {

    if(argc<2){
        printf("Must supply a database filename.\n");
        exit(EXIT_FAILURE);
    }
    char *filename = argv[1];
    Table *table = db_open(filename);

    print_init_prompt();
    InputBuffer *input_buffer = new_input_buffer();
    while (true) {
        print_prompt();
        read_input(input_buffer);

        //check_meta_data_command start with '.' .
        check_meta_data_command(input_buffer,table);

        Statement statement;
        switch (prepare_statement(input_buffer, &statement)) {
            case PREPARE_SUCCESS:
                break;
            case PREPARE_STRING_TOO_LONG:
                printf("String is too long.\n");
                continue;
            case PREPARE_SYNTAX_ERROR:
                printf("Syntext Error. Could not parse statement. \n");
                continue;
            case PREPARE_NEGATIVE_ID:
                printf("ID must be positive.\n");
                continue;
            case PREPARE_UNRECOGNIZED_STATEMENT:
                printf("Unrecognized keyword at start of '%s' . \n",
                       input_buffer->buffer);
                continue;
        }
        switch (execute_statement(&statement, table)) {
            case EXECUTE_TABLE_FULL:
                printf("Table Column is Filed.\n");
                continue;
            default:
                printf("Executed.\n");
                continue;

        };
    }

}




