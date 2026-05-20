#include "test.h"
#include "table.h"
#include "row.h"
#include "mem_alloc.h"
#include "index_hash.h"
#include "thread.h"
#include <filesystem>
namespace storage
{
	namespace {

		std::string resolve_schema_path() {
			namespace fs = std::filesystem;
			const fs::path source_dir = fs::path(__FILE__).parent_path();
			const fs::path repo_root = source_dir.parent_path().parent_path();
			const fs::path candidates[] = {
				fs::current_path() / "../Storage/test/data_schema.txt",
				fs::current_path() / "Storage/test/data_schema.txt",
				source_dir / "data_schema.txt",
				repo_root / "Storage/test/data_schema.txt",
			};
			for (const auto & candidate : candidates) {
				if (!candidate.empty() && fs::exists(candidate)) {
					return candidate.lexically_normal().string();
				}
			}
			return (source_dir / "data_schema.txt").lexically_normal().string();
		}

	} // namespace

	RC TestWorkload::init() {
		workload::init();
		string path = resolve_schema_path();
		init_schema(path.c_str() );

		if (the_table == nullptr || the_index == nullptr) assert(false);
		init_table();
		return RCOK;
	}

	RC TestWorkload::init_schema(const char * schema_file) {
		workload::init_schema(schema_file);
		the_table = tables["Data_TABLE"];
		the_index = indexes["Data_INDEX"];
		return RCOK;
	}

	RC TestWorkload::init_table() {
		RC rc = RCOK;
		for (int rid = 0; rid < 10; rid ++) {
			row_t * new_row = NULL;
			uint64_t row_id;
			int part_id = 0;
			rc = the_table->get_new_row(new_row, part_id, row_id);
			assert(rc == RCOK);
			uint64_t primary_key = rid;
			new_row->set_primary_key(primary_key);
			new_row->set_value(0, (void*) std::to_string(rid).c_str());
			new_row->set_value(1, (void*) std::to_string(rid).c_str());
			itemid_t * m_item = (itemid_t *) mem_allocator.alloc( sizeof(itemid_t), part_id );
			assert(m_item != NULL);
			m_item->type = DT_row;
			m_item->location = new_row;
			m_item->valid = true;
			uint64_t idx_key = primary_key;
			rc = the_index->index_insert(idx_key, m_item, 0);
			assert(rc == RCOK);
		}
		return rc;
	}

	RC TestWorkload::get_txn_man(txn_man *& txn_manager, thread_t * h_thd) {
		txn_manager = (TestTxnMan *)
			mem_allocator.alloc( sizeof(TestTxnMan), h_thd->get_thd_id() );
		new(txn_manager) TestTxnMan();
		txn_manager->init(h_thd, this, h_thd->get_thd_id());
		return RCOK;
	}

}
