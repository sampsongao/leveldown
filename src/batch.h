#ifndef LD_BATCH_H
#define LD_BATCH_H

#include <vector>
#include <napi.h>

#include <leveldb/write_batch.h>

#include "database.h"

namespace leveldown {

class Batch {
public:
  static void Init(napi_env env);
  static napi_value NewInstance (
      napi_env env
    , napi_value database
    , napi_value optionsObj
  );

  Batch  (leveldown::Database* database, bool sync);
  ~Batch ();
  leveldb::Status Write ();

private:
  leveldown::Database* database;
  leveldb::WriteOptions* options;
  leveldb::WriteBatch* batch;
  bool hasData; // keep track of whether we're writing data or not

  static void Destructor(napi_env env, void* obj, void* hint);

  static NAPI_METHOD(New);
  static NAPI_METHOD(Put);
  static NAPI_METHOD(Del);
  static NAPI_METHOD(Clear);
  static NAPI_METHOD(Write);
};

} // namespace leveldown

#endif
