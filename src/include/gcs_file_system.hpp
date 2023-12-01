//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// gcs_file_system.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {

struct GCSParsedUrl {
	string container;
	string prefix;
	string path;
};


class GCSStorageFileSystem : public FileSystem {
public:
	duckdb::unique_ptr<FileHandle> OpenFile(const string &path, uint8_t flags, FileLockType lock = DEFAULT_LOCK,
	                                        FileCompressionType compression = DEFAULT_COMPRESSION,
	                                        FileOpener *opener = nullptr) final;

	vector<string> Glob(const string &path, FileOpener *opener = nullptr) override;

	// FS methods
	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	void FileSync(FileHandle &handle) override;
	int64_t GetFileSize(FileHandle &handle) override;
	time_t GetLastModifiedTime(FileHandle &handle) override;
	bool FileExists(const string &filename) override;
	void Seek(FileHandle &handle, idx_t location) override;
	bool CanHandleFile(const string &fpath) override;

        bool CanSeek() override {
		return true;
	}
	bool OnDiskFile(FileHandle &handle) override {
		return false;
	}
	bool IsPipe(const string &filename) override {
		return false;
	}
	string GetName() const override {
		return "GCSFileSystem";
	}

	static void Verify();

protected:
	static GCSParsedUrl ParseUrl(const string &url);
//	static void ReadRange(FileHandle &handle, idx_t file_offset, char *buffer_out, idx_t buffer_out_len);
//	virtual duckdb::unique_ptr<AzureStorageFileHandle> CreateHandle(const string &path, uint8_t flags,
//	                                                                FileLockType lock, FileCompressionType compression,
//	                                                                FileOpener *opener);
};

} // namespace duckdb