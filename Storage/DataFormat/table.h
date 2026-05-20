//
// Created by zwx on 3/10/26.
//
#pragma once

#ifndef TABLE_H
#define TABLE_H

#include "../System/global.h"
namespace storage
{
    class Catalog;
    class row_t;

    class table_t {
    public:
        void init(Catalog * schema);
        // row lookup should be done with index. But index does not have
        // records for new rows. get_new_row returns the pointer to a
        // new row.
        RC get_new_row(row_t *& row); // this is equivalent to insert()
        RC get_new_row(row_t *& row, uint64_t part_id, uint64_t &row_id);

        void delete_row();

        uint64_t get_table_size() { return cur_tab_size; };
        Catalog * get_schema() { return schema; };
        const char * get_table_name() { return table_name; };

        Catalog * 		schema;
    private:
        const char * 	table_name;
        uint64_t  		cur_tab_size;
        char 			pad[CL_SIZE - sizeof(void *)*3];
    };
}

#endif //TABLE_H
