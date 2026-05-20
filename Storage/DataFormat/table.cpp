//
// Created by zwx on 3/10/26.
//

#include "../System/helper.h"
#include "../System/global.h"
#include "table.h"
#include "catalog.h"
#include "row.h"
#include "../System/mem_alloc.h"
namespace storage
{
    void table_t::init(Catalog * schema) {
        this->table_name = schema->table_name;
        this->schema = schema;
    }

    RC table_t::get_new_row(row_t *& row) {
        // this function is obsolete.
        assert(false);
        return RCOK;
    }

    // the row is not stored locally. the pointer must be maintained by index structure.
    RC table_t::get_new_row(row_t *& row, uint64_t part_id, uint64_t &row_id) {
        RC rc = RCOK;
        row_id = cur_tab_size;
        cur_tab_size ++;
        row = (row_t *) _mm_malloc(sizeof(row_t), 64);
        rc = row->init(this, part_id, row_id);
        row->init_manager(row);

        return rc;
    }
}