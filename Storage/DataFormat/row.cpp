//
// Created by zwx on 3/10/26.
//

#include <mm_malloc.h>
#include "../System/global.h"
#include "table.h"
#include "catalog.h"
#include "row.h"
#include "../System/txn.h"
#include "../concurrency_control/occ.h"
#include "../System/mem_alloc.h"
#include "../System/manager.h"
namespace storage
{
	RC row_t::init(table_t * host_table, uint64_t part_id, uint64_t row_id) {
		_row_id = row_id;
		_part_id = part_id;
		this->table = host_table;
		Catalog * schema = host_table->get_schema();
		int tuple_size = schema->get_tuple_size();
		data = (char *) _mm_malloc(sizeof(char) * tuple_size, 64);
		return RCOK;
	}
	void row_t::init(int size)
	{
		data = (char *) _mm_malloc(size, 64);
	}

	RC row_t::switch_schema(table_t * host_table) {
		this->table = host_table;
		return RCOK;
	}

	void row_t::init_manager(row_t * row) {
		manager = (Row_occ *) mem_allocator.alloc(sizeof(Row_occ), _part_id);
		manager->init(this);
	}

	table_t * row_t::get_table() {
		return table;
	}

	Catalog * row_t::get_schema() {
		return get_table()->get_schema();
	}

	const char * row_t::get_table_name() {
		return get_table()->get_table_name();
	};
	uint64_t row_t::get_tuple_size() {
		return get_schema()->get_tuple_size();
	}

	uint64_t row_t::get_field_cnt() {
		return get_schema()->field_cnt;
	}

	void row_t::set_value(int id, void * ptr) {
		int datasize = get_schema()->get_field_size(id);
		int pos = get_schema()->get_field_index(id);
		const char * field_type = get_schema()->get_field_type(id);
		if (field_type != nullptr && strcmp(field_type, "string") == 0) {
			memset(&data[pos], 0, datasize);
			if (ptr != nullptr && datasize > 0) {
				strncpy(&data[pos], static_cast<const char *>(ptr), datasize - 1);
			}
			return;
		}
		memcpy(&data[pos], ptr, datasize);
	}

	void row_t::set_value(int id, void * ptr, int size) {
		int datasize = get_schema()->get_field_size(id);
		int pos = get_schema()->get_field_index(id);
		int copy_size = size < datasize ? size : datasize;
		memcpy(&data[pos], ptr, copy_size);
		if (copy_size < datasize) {
			memset(&data[pos + copy_size], 0, datasize - copy_size);
		}
	}

	void row_t::set_value(const char * col_name, void * ptr) {
		uint64_t id = get_schema()->get_field_id(col_name);
		set_value(id, ptr);
	}

	SET_VALUE(uint64_t);
	SET_VALUE(int64_t);
	SET_VALUE(double);
	SET_VALUE(UInt32);
	SET_VALUE(SInt32);

	GET_VALUE(uint64_t);
	GET_VALUE(int64_t);
	GET_VALUE(double);
	GET_VALUE(UInt32);
	GET_VALUE(SInt32);

	DECL_SET_VALUE(uint64_t);
	DECL_SET_VALUE(int64_t);
	DECL_SET_VALUE(double);
	DECL_SET_VALUE(UInt32);
	DECL_SET_VALUE(SInt32);

	char * row_t::get_value(int id) {
		int pos = get_schema()->get_field_index(id);
		return &data[pos];
	}

	char * row_t::get_value(char * col_name) {
		uint64_t pos = get_schema()->get_field_index(col_name);
		return &data[pos];
	}

	char * row_t::get_data() { return data; }

	void row_t::set_data(char * data, uint64_t size) {
		memcpy(this->data, data, size);
	}
	// copy from the src to this
	void row_t::copy(row_t * src) {
		set_data(src->get_data(), src->get_tuple_size());
	}

	void row_t::free_row() {
		free(data);
	}

	RC row_t::get_row(access_t type, txn_man * txn, row_t *& row) {
		RC rc = RCOK;
		// OCC always make a local copy regardless of read or write
		txn->cur_row = (row_t *) mem_allocator.alloc(sizeof(row_t), get_part_id());
		txn->cur_row->init(get_table(), get_part_id());
		rc = this->manager->access(txn, R_REQ);
		row = txn->cur_row;
		return rc;
	}

	void row_t::return_row(access_t type, txn_man * txn, row_t * row) {
		assert (row != NULL);
		if (type == WR)
		{
			manager->write( row, txn->end_ts );
		}

		row->free_row();
		mem_allocator.free(row, sizeof(row_t));
	}

}
