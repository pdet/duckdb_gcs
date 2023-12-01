#include "gcs_file_system.hpp"
#include "gcs_extension.hpp"
#include "gcs_file_handle.hpp"

namespace duckdb {

unique_ptr<AzureStorageFileHandle> GCSStorageFileSystem::CreateHandle(const string &path, uint8_t flags,
                                                                        FileLockType lock,
                                                                        FileCompressionType compression,
                                                                        FileOpener *opener) {
	D_ASSERT(compression == FileCompressionType::UNCOMPRESSED);

	auto parsed_url = ParseUrl(path);
	auto azure_auth = ParseAzureAuthSettings(opener);
	auto read_options = ParseAzureReadOptions(opener);

	return make_uniq<AzureStorageFileHandle>(*this, path, flags, azure_auth, read_options, parsed_url);
}

unique_ptr<FileHandle> GCSStorageFileSystem::OpenFile(const string &path, uint8_t flags, FileLockType lock,
                                                        FileCompressionType compression, FileOpener *opener) {
	D_ASSERT(compression == FileCompressionType::UNCOMPRESSED);

	if (flags & FileFlags::FILE_FLAGS_WRITE) {
		throw NotImplementedException("Writing to gcs containers is currently not supported");
	}

	auto handle = CreateHandle(path, flags, lock, compression, opener);
	return std::move(handle);
}

int64_t GCSStorageFileSystem::GetFileSize(FileHandle &handle) {
	auto &afh = (GCSFileHandle &)handle;
	return afh.length;
}

time_t GCSStorageFileSystem::GetLastModifiedTime(FileHandle &handle) {
	auto &afh = (GCSFileHandle &)handle;
	return afh.last_modified;
}

bool GCSStorageFileSystem::CanHandleFile(const string &fpath) {
	return fpath.rfind("gs://", 0) == 0;
}

void GCSStorageFileSystem::Seek(FileHandle &handle, idx_t location) {
	auto &sfh = (GCSFileHandle &)handle;
	sfh.file_offset = location;
}

void GCSStorageFileSystem::FileSync(FileHandle &handle) {
	throw NotImplementedException("FileSync for GCS files not implemented");
}

static void LoadInternal(DatabaseInstance &instance) {
	// Load filesystem
	auto &fs = instance.GetFileSystem();
	fs.RegisterSubSystem(make_uniq<GCSStorageFileSystem>());

//	// Load extension config
//	auto &config = DBConfig::GetConfig(instance);
//	config.AddExtensionOption("azure_storage_connection_string",
//	                          "Azure connection string, used for authenticating and configuring azure requests",
//	                          LogicalType::VARCHAR);
//	config.AddExtensionOption(
//	    "azure_account_name",
//	    "Azure account name, when set, the extension will attempt to automatically detect credentials",
//	    LogicalType::VARCHAR);
//	config.AddExtensionOption("azure_credential_chain",
//	                          "Ordered list of Azure credential providers, in string format separated by ';'. E.g. "
//	                          "'cli;managed_identity;env'",
//	                          LogicalType::VARCHAR, "none");
//	config.AddExtensionOption("azure_endpoint",
//	                          "Override the azure endpoint for when the Azure credential providers are used.",
//	                          LogicalType::VARCHAR, "blob.core.windows.net");
//
//	AzureReadOptions default_read_options;
//	config.AddExtensionOption("azure_read_transfer_concurrency",
//	                          "Maximum number of threads the Azure client can use for a single parallel read. "
//				  "If azure_read_transfer_chunk_size is less than azure_read_buffer_size then setting "
//				  "this > 1 will allow the Azure client to do concurrent requests to fill the buffer.",
//	                          LogicalType::INTEGER, Value::INTEGER(default_read_options.transfer_concurrency));
//
//	config.AddExtensionOption("azure_read_transfer_chunk_size",
//	                          "Maximum size in bytes that the Azure client will read in a single request. "
//				  "It is recommended that this is a factor of azure_read_buffer_size.",
//	                          LogicalType::BIGINT, Value::BIGINT(default_read_options.transfer_chunk_size));
//
//	config.AddExtensionOption("azure_read_buffer_size",
//	                          "Size of the read buffer.  It is recommended that this is evenly divisible by "
//				  "azure_read_transfer_chunk_size.",
//	                          LogicalType::UBIGINT, Value::UBIGINT(default_read_options.buffer_size));
}

int64_t GCSStorageFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &hfh = (AzureStorageFileHandle &)handle;
	idx_t max_read = hfh.length - hfh.file_offset;
	nr_bytes = MinValue<idx_t>(max_read, nr_bytes);
	Read(handle, buffer, nr_bytes, hfh.file_offset);
	return nr_bytes;
}


vector<string> GCSStorageFileSystem::Glob(const string &path, FileOpener *opener) {

        throw InternalException("Cannot do GCS glob yet");

}

// TODO: this code is identical to HTTPFS, look into unifying it
void GCSStorageFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto &hfh = (AzureStorageFileHandle &)handle;

	idx_t to_read = nr_bytes;
	idx_t buffer_offset = 0;

	// Don't buffer when DirectIO is set.
	if (hfh.flags & FileFlags::FILE_FLAGS_DIRECT_IO && to_read > 0) {
		ReadRange(hfh, location, (char *)buffer, to_read);
		hfh.buffer_available = 0;
		hfh.buffer_idx = 0;
		hfh.file_offset = location + nr_bytes;
		return;
	}

	if (location >= hfh.buffer_start && location < hfh.buffer_end) {
		hfh.file_offset = location;
		hfh.buffer_idx = location - hfh.buffer_start;
		hfh.buffer_available = (hfh.buffer_end - hfh.buffer_start) - hfh.buffer_idx;
	} else {
		// reset buffer
		hfh.buffer_available = 0;
		hfh.buffer_idx = 0;
		hfh.file_offset = location;
	}
	while (to_read > 0) {
		auto buffer_read_len = MinValue<idx_t>(hfh.buffer_available, to_read);
		if (buffer_read_len > 0) {
			D_ASSERT(hfh.buffer_start + hfh.buffer_idx + buffer_read_len <= hfh.buffer_end);
			memcpy((char *)buffer + buffer_offset, hfh.read_buffer.get() + hfh.buffer_idx, buffer_read_len);

			buffer_offset += buffer_read_len;
			to_read -= buffer_read_len;

			hfh.buffer_idx += buffer_read_len;
			hfh.buffer_available -= buffer_read_len;
			hfh.file_offset += buffer_read_len;
		}

		if (to_read > 0 && hfh.buffer_available == 0) {
			auto new_buffer_available = MinValue<idx_t>(hfh.read_options.buffer_size, hfh.length - hfh.file_offset);

			// Bypass buffer if we read more than buffer size
			if (to_read > new_buffer_available) {
				ReadRange(hfh, location + buffer_offset, (char *)buffer + buffer_offset, to_read);
				hfh.buffer_available = 0;
				hfh.buffer_idx = 0;
				hfh.file_offset += to_read;
				break;
			} else {
				ReadRange(hfh, hfh.file_offset, (char *)hfh.read_buffer.get(), new_buffer_available);
				hfh.buffer_available = new_buffer_available;
				hfh.buffer_idx = 0;
				hfh.buffer_start = hfh.file_offset;
				hfh.buffer_end = hfh.buffer_start + new_buffer_available;
			}
		}
	}
}

bool GCSStorageFileSystem::FileExists(const string &filename) {
	try {
		auto handle = OpenFile(filename, FileFlags::FILE_FLAGS_READ);
		auto &sfh = (GCSFileHandle &)*handle;
		if (sfh.length == 0) {
			return false;
		}
		return true;
	} catch (...) {
		return false;
	};
}



GCSParsedUrl GCSStorageFileSystem::ParseUrl(const string &url) {
	string container, prefix, path;

	if (url.rfind("gs://", 0) != 0) {
		throw IOException("URL needs to start with gs://");
	}
	auto prefix_end_pos = url.find("//") + 2;
	auto slash_pos = url.find('/', prefix_end_pos);
	if (slash_pos == string::npos) {
		throw IOException("URL needs to contain a '/' after the host");
	}
	container = url.substr(prefix_end_pos, slash_pos - prefix_end_pos);
	if (container.empty()) {
		throw IOException("URL needs to contain a bucket name");
	}

	prefix = url.substr(0, prefix_end_pos);
	path = url.substr(slash_pos + 1);
	return {container, prefix, path};
}

} // namespace duckdb