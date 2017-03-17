#include <napi.h>

#include "database.h"
#include "batch_async.h"
#include "batch.h"
#include "common.h"

namespace leveldown {

static napi_ref batch_constructor;

Batch::Batch (leveldown::Database* database, bool sync) : database(database) {
  options = new leveldb::WriteOptions();
  options->sync = sync;
  batch = new leveldb::WriteBatch();
  hasData = false;
}

Batch::~Batch () {
  delete options;
  delete batch;
}

void Batch::Destructor(void* obj, void* hint) {
  Batch* batch = static_cast<Batch*>(obj);
  delete batch;
}

leveldb::Status Batch::Write () {
  return database->WriteBatchToDatabase(options, batch);
}

void Batch::Init (napi_env env) {
  napi_property_descriptor methods[] = {
    { "put", Batch::Put },
    { "del", Batch::Del },
    { "clear", Batch::Clear },
    { "write", Batch::Write },
  };

  napi_value ctor;
  CHECK_NAPI_RESULT(napi_define_class(env, "Batch", Batch::New, nullptr, 4, methods, &ctor));
  CHECK_NAPI_RESULT(napi_create_reference(env, ctor, 1, &batch_constructor));
}

NAPI_METHOD(Batch::New) {
  napi_value args[2];

  CHECK_NAPI_RESULT(napi_get_cb_args(env, info, args, 2));
  int argsLength;
  CHECK_NAPI_RESULT(napi_get_cb_args_length(env, info, &argsLength));
  napi_value _this;
  CHECK_NAPI_RESULT(napi_get_cb_this(env, info, &_this));

  void* unwrapped;
  CHECK_NAPI_RESULT(napi_unwrap(env, args[0], &unwrapped));
  Database* database = static_cast<Database*>(unwrapped);
  napi_value optionsObj = nullptr;

  napi_valuetype t;
  CHECK_NAPI_RESULT(napi_get_type_of_value(env, args[1], &t));

  if (argsLength > 1 && t == napi_object) {
    optionsObj = args[1];
  }

  bool sync = BooleanOptionValue(env, optionsObj, "sync");

  Batch* batch = new Batch(database, sync);

  CHECK_NAPI_RESULT(napi_wrap(env, _this, batch, Batch::Destructor, nullptr, nullptr));
  CHECK_NAPI_RESULT(napi_set_return_value(env, info, _this));
}

napi_value Batch::NewInstance (
        napi_env env
      , napi_value database
      , napi_value optionsObj
    ) {

  Napi::EscapableHandleScope scope(env);

  napi_value instance;

  napi_value constructorHandle;
  CHECK_NAPI_RESULT(napi_get_reference_value(env, batch_constructor, &constructorHandle));

  if (optionsObj == nullptr) {
    napi_value argv[1] = { database };
    CHECK_NAPI_RESULT(napi_new_instance(env, constructorHandle, 1, argv, &instance));
  } else {
    napi_value argv[2] = { database, optionsObj };
    CHECK_NAPI_RESULT(napi_new_instance(env, constructorHandle, 2, argv, &instance));
  }

  return scope.Escape(instance);
}

NAPI_METHOD(Batch::Put) {
  napi_value args[2];
  napi_get_cb_args(env, info, args, 2);
  napi_value holder;
  CHECK_NAPI_RESULT(napi_get_cb_holder(env, info, &holder));

  void* wrapped;
  CHECK_NAPI_RESULT(napi_unwrap(env, holder, &wrapped));
  Batch* batch = static_cast<Batch*>(wrapped);

  napi_value keyBuffer = args[0];
  napi_value valueBuffer = args[1];
  LD_STRING_OR_BUFFER_TO_SLICE(key, keyBuffer, key)
  LD_STRING_OR_BUFFER_TO_SLICE(value, valueBuffer, value)

  batch->batch->Put(key, value);
  if (!batch->hasData)
    batch->hasData = true;

  DisposeStringOrBufferFromSlice(env, keyBuffer, key);
  DisposeStringOrBufferFromSlice(env, valueBuffer, value);

  CHECK_NAPI_RESULT(napi_set_return_value(env, info, holder));
}

NAPI_METHOD(Batch::Del) {
  napi_value args[1];
  napi_get_cb_args(env, info, args, 1);
  napi_value holder;
  CHECK_NAPI_RESULT(napi_get_cb_holder(env, info, &holder));
  void* unwrapped;
  CHECK_NAPI_RESULT(napi_unwrap(env, holder, &unwrapped));
  Batch* batch = static_cast<Batch*>(unwrapped);

  napi_value keyBuffer = args[0];
  LD_STRING_OR_BUFFER_TO_SLICE(key, keyBuffer, key)

  batch->batch->Delete(key);
  if (!batch->hasData)
    batch->hasData = true;

  DisposeStringOrBufferFromSlice(env, keyBuffer, key);

  CHECK_NAPI_RESULT(napi_set_return_value(env, info, holder));
}

NAPI_METHOD(Batch::Clear) {
  napi_value holder;
  CHECK_NAPI_RESULT(napi_get_cb_holder(env, info, &holder));
  void* unwrapped;
  CHECK_NAPI_RESULT(napi_unwrap(env, holder, &unwrapped));
  Batch* batch = static_cast<Batch*>(unwrapped);

  batch->batch->Clear();
  batch->hasData = false;

  CHECK_NAPI_RESULT(napi_set_return_value(env, info, holder));
}

NAPI_METHOD(Batch::Write) {
  napi_value args[1];
  CHECK_NAPI_RESULT(napi_get_cb_args(env, info, args, 1));
  napi_value _this;
  CHECK_NAPI_RESULT(napi_get_cb_this(env, info, &_this));
  napi_value holder;
  CHECK_NAPI_RESULT(napi_get_cb_holder(env, info, &holder));
  void* unwrapped;
  CHECK_NAPI_RESULT(napi_unwrap(env, holder, &unwrapped));
  Batch* batch = static_cast<Batch*>(unwrapped);

  if (batch->hasData) {
    napi_value callback = args[0];
    BatchWriteWorker* worker  = new BatchWriteWorker(batch, env, callback);
    // persist to prevent accidental GC
    worker->Persistent().Set("batch", _this);
    worker->Queue();
  } else {
    LD_RUN_CALLBACK(args[0], 0, NULL);
  }
}

} // namespace leveldown
