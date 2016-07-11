#include <node.h>
#include <node_buffer.h>
#include <nan.h>

#include "database.h"
#include "batch_async.h"
#include "batch.h"
#include "common.h"

namespace leveldown {

static napi_persistent batch_constructor;

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

void Batch::Destructor(void* obj) {
  Batch* batch = static_cast<Batch*>(obj);
  delete batch;
}

leveldb::Status Batch::Write () {
  return database->WriteBatchToDatabase(options, batch);
}

void Batch::Init (napi_env env) {
  napi_value ctor = napi_create_constructor_for_wrap(env, Batch::New);
  napi_set_function_name(env, ctor, napi_property_name(env, "Batch"));

  // Is this used? Is it for ObjectWrap?
  //tpl->InstanceTemplate()->SetInternalFieldCount(1);

  napi_value proto = napi_get_property(env, ctor, napi_property_name(env, "prototype"));

  napi_value fnPut = napi_create_function(env, Batch::Put);
  napi_propertyname pnPut = napi_property_name(env, "put");
  napi_set_function_name(env, fnPut, pnPut);
  napi_set_property(env, proto, pnPut, fnPut);

  napi_value fnDel = napi_create_function(env, Batch::Del);
  napi_propertyname pnDel = napi_property_name(env, "del");
  napi_set_function_name(env, fnDel, pnDel);
  napi_set_property(env, proto, pnDel, fnDel);

  napi_value fnClear = napi_create_function(env, Batch::Clear);
  napi_propertyname pnClear = napi_property_name(env, "clear");
  napi_set_function_name(env, fnClear, pnClear);
  napi_set_property(env, proto, pnClear, fnClear);

  napi_value fnWrite = napi_create_function(env, Batch::Write);
  napi_propertyname pnWrite = napi_property_name(env, "write");
  napi_set_function_name(env, fnWrite, pnWrite);
  napi_set_property(env, proto, pnWrite, fnWrite);

  batch_constructor = napi_create_persistent(env, ctor);
}

NAPI_METHOD(Batch::New) {
  napi_value args[2];
  napi_get_cb_args(env, info, args, 2);
  int argsLength = napi_get_cb_args_length(env, info);
  napi_value _this = napi_get_cb_this(env, info);

  Database* database = static_cast<Database*>(napi_unwrap(env, args[0]));
  napi_value optionsObj = nullptr;

  if (argsLength > 1 && napi_get_type_of_value(env, args[1]) == napi_object) {
    optionsObj = args[1];
  }

  bool sync = BooleanOptionValue(env, optionsObj, "sync");

  Batch* batch = new Batch(database, sync);
  napi_wrap(env, _this, batch, Batch::Destructor, nullptr);

  napi_set_return_value(env, info, _this);
}

napi_value Batch::NewInstance (
        napi_env env
      , napi_value database
      , napi_value optionsObj
    ) {

  Napi::EscapableHandleScope scope(env);

  napi_value instance;

  napi_value constructorHandle = napi_get_persistent_value(env, batch_constructor);

  if (optionsObj == nullptr) {
    napi_value argv[1] = { database };
    instance = napi_new_instance(env, constructorHandle, 1, argv);
  } else {
    napi_value argv[2] = { database, optionsObj };
    instance = napi_new_instance(env, constructorHandle, 2, argv);
  }

  return scope.Escape(instance);
}

NAPI_METHOD(Batch::Put) {
  napi_value args[2];
  napi_get_cb_args(env, info, args, 2);
  napi_value holder = napi_get_cb_holder(env, info);
  Batch* batch = static_cast<Batch*>(napi_unwrap(env, holder));

  napi_value keyBuffer = args[0];
  napi_value valueBuffer = args[1];
  LD_STRING_OR_BUFFER_TO_SLICE(key, keyBuffer, key)
  LD_STRING_OR_BUFFER_TO_SLICE(value, valueBuffer, value)

  batch->batch->Put(key, value);
  if (!batch->hasData)
    batch->hasData = true;

  DisposeStringOrBufferFromSlice(keyBuffer, key);
  DisposeStringOrBufferFromSlice(valueBuffer, value);

  napi_set_return_value(env, info, holder);
}

NAPI_METHOD(Batch::Del) {
  napi_value args[1];
  napi_get_cb_args(env, info, args, 1);
  napi_value holder = napi_get_cb_holder(env, info);
  Batch* batch = static_cast<Batch*>(napi_unwrap(env, holder));

  napi_value keyBuffer = args[0];
  LD_STRING_OR_BUFFER_TO_SLICE(key, keyBuffer, key)

  batch->batch->Delete(key);
  if (!batch->hasData)
    batch->hasData = true;

  DisposeStringOrBufferFromSlice(keyBuffer, key);

  napi_set_return_value(env, info, holder);
}

NAPI_METHOD(Batch::Clear) {
  napi_value holder = napi_get_cb_holder(env, info);
  Batch* batch = static_cast<Batch*>(napi_unwrap(env, holder));

  batch->batch->Clear();
  batch->hasData = false;

  napi_set_return_value(env, info, holder);
}

NAPI_METHOD(Batch::Write) {
  napi_value args[1];
  napi_get_cb_args(env, info, args, 1);
  napi_value _this  = napi_get_cb_this(env, info);
  napi_value holder = napi_get_cb_holder(env, info);
  Batch* batch = static_cast<Batch*>(napi_unwrap(env, holder));

  if (batch->hasData) {
    napi_value callback = args[0];
    BatchWriteWorker* worker  = new BatchWriteWorker(batch, callback);
    // persist to prevent accidental GC
    worker->SaveToPersistent("batch", _this);
    Napi::AsyncQueueWorker(worker);
  } else {
    LD_RUN_CALLBACK(args[0], 0, NULL);
  }
}

} // namespace leveldown
