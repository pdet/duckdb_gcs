//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// gcs_file_handle.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {


class GCSFileHandle : public FileHandle {
public:
	GCSFileHandle(FileSystem &fs, string path, uint8_t flags, AzureAuthentication &auth,
	                       const AzureReadOptions &read_options, AzureParsedUrl parsed_url);
	~GCSFileHandle() override = default;

public:
	void Close() override {
	}

	uint8_t flags;
	idx_t length;
	time_t last_modified;

	// Read info
	idx_t buffer_available;
	idx_t buffer_idx;
	idx_t file_offset;
	idx_t buffer_start;
	idx_t buffer_end;

	// Read buffer
	duckdb::unique_ptr<data_t[]> read_buffer;

	// Azure Blob Client
	BlobClientWrapper blob_client;

	AzureReadOptions read_options;

};
}