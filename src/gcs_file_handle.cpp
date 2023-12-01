#include "gcs_file_handle.hpp"

namespace duckdb{
GCSFileHandle::GCSFileHandle(FileSystem &fs, string path_p, uint8_t flags, AzureAuthentication &auth,
                                               const AzureReadOptions &read_options, AzureParsedUrl parsed_url)
    : FileHandle(fs, std::move(path_p)), flags(flags), length(0), last_modified(time_t()), buffer_available(0),
      buffer_idx(0), file_offset(0), buffer_start(0), buffer_end(0), blob_client(auth, parsed_url),
      read_options(read_options) {
	try {
		auto client = *blob_client.GetClient();
		auto res = client.GetProperties();
		length = res.Value.BlobSize;
	} catch (Azure::Storage::StorageException &e) {
		throw IOException("AzureStorageFileSystem open file '" + path + "' failed with code'" + e.ErrorCode +
		                  "',Reason Phrase: '" + e.ReasonPhrase + "', Message: '" + e.Message + "'");
	}

	if (flags & FileFlags::FILE_FLAGS_READ) {
		read_buffer = duckdb::unique_ptr<data_t[]>(new data_t[read_options.buffer_size]);
	}
}

}