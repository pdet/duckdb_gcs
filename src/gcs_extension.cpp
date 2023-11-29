#include "gcs_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include <google/cloud/storage/client.h>

namespace duckdb {

struct SetCredentialsResult {
  std::string set_access_key_id;
  std::string set_secret_access_key;
  std::string set_session_token;
  std::string set_region;
};

//! Set the DuckDB GCS Credentials
static SetCredentialsResult
TrySetGCSCredentials(DBConfig &config, const std::string &json_key_file) {
  // TODO: Add your GCS credential logic here using the Google Cloud C++ Client
  // Library Example: google::cloud::storage::Client
  // client(google::cloud::storage::ClientOptions::Builder(
  //     google::cloud::storage::ClientOptions::FromJsonFile(json_key_file)));
  // auto credentials = client.GetCredentials();

  SetCredentialsResult ret;
  // TODO: Extract relevant information from credentials and set it in the
  // 'config'

  return ret;
}

struct SetGCSCredentialsFunctionData : public TableFunctionData {
  std::string json_key_file;
  bool finished = false;
};

unique_ptr<FunctionData>
LoadGCSCredentialsBind(ClientContext &context, TableFunctionBindInput &input,
                       vector<LogicalType> &return_types,
                       vector<string> &names) {
  auto result = std::make_unique<SetGCSCredentialsFunctionData>();

  if (input.inputs.size() >= 1) {
    result->json_key_file = input.inputs[0].ToString();
  }

  return_types.emplace_back(LogicalType::VARCHAR);
  names.emplace_back("loaded_access_key_id");

  return_types.emplace_back(LogicalType::VARCHAR);
  names.emplace_back("loaded_secret_access_key");

  return_types.emplace_back(LogicalType::VARCHAR);
  names.emplace_back("loaded_session_token");

  return_types.emplace_back(LogicalType::VARCHAR);
  names.emplace_back("loaded_region");

  return result;
}

static void LoadGCSCredentialsFun(ClientContext &context,
                                  TableFunctionInput &data_p,
                                  DataChunk &output) {
  auto &data =
      static_cast<const SetGCSCredentialsFunctionData &>(*data_p.bind_data);
  if (data.finished) {
    return;
  }

  if (!Catalog::TryAutoLoad(context, "gcs")) {
    throw MissingExtensionException(
        "gcs extension is required for load_gcs_credentials");
  }

  auto load_result =
      TrySetGCSCredentials(DBConfig::GetConfig(context), data.json_key_file);

  // Set return values for all modified params
  output.SetValue(0, 0,
                  load_result.set_access_key_id.empty()
                      ? Value(nullptr)
                      : load_result.set_access_key_id);
  output.SetValue(1, 0,
                  load_result.set_secret_access_key.empty()
                      ? Value(nullptr)
                      : load_result.set_secret_access_key);
  output.SetValue(2, 0,
                  load_result.set_session_token.empty()
                      ? Value(nullptr)
                      : load_result.set_session_token);
  output.SetValue(3, 0,
                  load_result.set_region.empty() ? Value(nullptr)
                                                 : load_result.set_region);

  output.SetCardinality(1);

  //    data.finished = true;
}

static void LoadInternal(DuckDB &db) {
  TableFunctionSet function_set("load_gcs_credentials");
  auto base_fun = TableFunction("load_gcs_credentials", {},
                                LoadGCSCredentialsFun, LoadGCSCredentialsBind);

  function_set.AddFunction(base_fun);

  ExtensionUtil::RegisterFunction(*db.instance, function_set);
}

void GcsExtension::Load(DuckDB &db) { LoadInternal(db); }

std::string GcsExtension::Name() { return "gcs"; }

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void gcs_init(duckdb::DatabaseInstance &db) {
  duckdb::DuckDB db_wrapper(db);
  db_wrapper.LoadExtension<duckdb::GcsExtension>();
}

DUCKDB_EXTENSION_API const char *gcs_version() {
  return duckdb::DuckDB::LibraryVersion();
}
}
