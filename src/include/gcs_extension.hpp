//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// gcs_extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {

struct GCSAuthentication {
	//! Auth method #1: setting the connection string
	string connection_string;

	//! Auth method #2: setting account name + defining a credential chain.
	string account_name;
	string credential_chain;
	string endpoint;
};

struct GCSReadOptions {
	int32_t transfer_concurrency = 5;
	int64_t transfer_chunk_size = 1 * 1024 * 1024;
	idx_t buffer_size = 1 * 1024 * 1024;
};


class GcsExtension : public Extension {
public:
  void Load(DuckDB &db) override;
  std::string Name() override;
};

} // namespace duckdb
