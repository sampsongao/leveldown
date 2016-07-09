/* Copyright (c) 2012-2016 LevelDOWN contributors
 * See list at <https://github.com/level/leveldown#contributing>
 * MIT License <https://github.com/level/leveldown/blob/master/LICENSE.md>
 */

#include <node.h>
#include <node_jsvmapi.h>
#include <node_buffer.h>

#include <leveldb/db.h>
#include <leveldb/write_batch.h>

#include "leveldown.h"
#include "database.h"
#include "async.h"
#include "database_async.h"
#include "batch.h"
#include "iterator.h"
#include "common.h"

namespace leveldown {

static napi_persistent database_constructor;

Database::Database (napi_value from)
  : location(new Napi::Utf8String(from))
  , db(NULL)
  , currentIteratorId(0)
  , pendingCloseWorker(NULL)
  , blockCache(NULL)
  , filterPolicy(NULL) {};

Database::~Database () {
  if (db != NULL)
    delete db;
  delete location;
};

/* Calls from worker threads, NO V8 HERE *****************************/

leveldb::Status Database::OpenDatabase (
        leveldb::Options* options
    ) {
  return leveldb::DB::Open(*options, **location, &db);
}

leveldb::Status Database::PutToDatabase (
        leveldb::WriteOptions* options
      , leveldb::Slice key
      , leveldb::Slice value
    ) {
  return db->Put(*options, key, value);
}

leveldb::Status Database::GetFromDatabase (
        leveldb::ReadOptions* options
      , leveldb::Slice key
      , std::string& value
    ) {
  return db->Get(*options, key, &value);
}

leveldb::Status Database::DeleteFromDatabase (
        leveldb::WriteOptions* options
      , leveldb::Slice key
    ) {
  return db->Delete(*options, key);
}

leveldb::Status Database::WriteBatchToDatabase (
        leveldb::WriteOptions* options
      , leveldb::WriteBatch* batch
    ) {
  return db->Write(*options, batch);
}

uint64_t Database::ApproximateSizeFromDatabase (const leveldb::Range* range) {
  uint64_t size;
  db->GetApproximateSizes(range, 1, &size);
  return size;
}

void Database::GetPropertyFromDatabase (
      const leveldb::Slice& property
    , std::string* value) {

  db->GetProperty(property, value);
}

leveldb::Iterator* Database::NewIterator (leveldb::ReadOptions* options) {
  return db->NewIterator(*options);
}

const leveldb::Snapshot* Database::NewSnapshot () {
  return db->GetSnapshot();
}

void Database::ReleaseSnapshot (const leveldb::Snapshot* snapshot) {
  return db->ReleaseSnapshot(snapshot);
}

void Database::ReleaseIterator (uint32_t id) {
  // called each time an Iterator is End()ed, in the main thread
  // we have to remove our reference to it and if it's the last iterator
  // we have to invoke a pending CloseWorker if there is one
  // if there is a pending CloseWorker it means that we're waiting for
  // iterators to end before we can close them
  iterators.erase(id);
  if (iterators.empty() && pendingCloseWorker != NULL) {
    Napi::AsyncQueueWorker((AsyncWorker*)pendingCloseWorker);
    pendingCloseWorker = NULL;
  }
}

void Database::CloseDatabase () {
  delete db;
  db = NULL;
  if (blockCache) {
    delete blockCache;
    blockCache = NULL;
  }
  if (filterPolicy) {
    delete filterPolicy;
    filterPolicy = NULL;
  }
}

/* V8 exposed functions *****************************/

void LevelDOWN (napi_env env, napi_func_cb_info info) {
  napi_value args[1];
  napi_get_cb_args(env, info, args, 1);

  napi_value location = args[0];

  napi_set_return_value(env, info, Database::NewInstance(env, location));
}

void Database::Init (napi_env env) {
  napi_value ctor = napi_create_constructor_for_wrap(env, Database::New);
  napi_set_function_name(env, ctor, napi_property_name(env, "Database"));
  // Was this never used?
  //tpl->InstanceTemplate()->SetInternalFieldCount(1);
  napi_value proto = napi_get_property(env, ctor, napi_property_name(env, "prototype"));

  // Concern (ianhall): This verbose setting of functions on the prototype object defeats V8's Function/ObjectTemplate optimization
  // Also very chatty.  Creating a constructor and setting methods on its prototype object should be a single API.

  napi_value fnOpen = napi_create_function(env, Database::Open);
  napi_propertyname pnOpen = napi_property_name(env, "open");
  napi_set_function_name(env, fnOpen, pnOpen);
  napi_set_property(env, proto, pnOpen, fnOpen);

  napi_value fnClose = napi_create_function(env, Database::Close);
  napi_propertyname pnClose = napi_property_name(env, "close");
  napi_set_function_name(env, fnClose, pnClose);
  napi_set_property(env, proto, pnClose, fnClose);

  napi_value fnPut = napi_create_function(env, Database::Put);
  napi_propertyname pnPut = napi_property_name(env, "put");
  napi_set_function_name(env, fnPut, pnPut);
  napi_set_property(env, proto, pnPut, fnPut);

  napi_value fnGet = napi_create_function(env, Database::Get);
  napi_propertyname pnGet = napi_property_name(env, "get");
  napi_set_function_name(env, fnGet, pnGet);
  napi_set_property(env, proto, pnGet, fnGet);

  napi_value fnDelete = napi_create_function(env, Database::Delete);
  napi_propertyname pnDel = napi_property_name(env, "del");
  napi_set_function_name(env, fnDelete, pnDel);
  napi_set_property(env, proto, pnDel, fnDelete);

  napi_value fnBatch = napi_create_function(env, Database::Batch);
  napi_propertyname pnBatch = napi_property_name(env, "batch");
  napi_set_function_name(env, fnBatch, pnBatch);
  napi_set_property(env, proto, pnBatch, fnBatch);

  napi_value fnApproximateSize = napi_create_function(env, Database::ApproximateSize);
  napi_propertyname pnApproximateSize = napi_property_name(env, "approximateSize");
  napi_set_function_name(env, fnApproximateSize, pnApproximateSize);
  napi_set_property(env, proto, pnApproximateSize, fnApproximateSize);

  napi_value fnGetProperty = napi_create_function(env, Database::GetProperty);
  napi_propertyname pnGetProperty = napi_property_name(env, "getProperty");
  napi_set_function_name(env, fnGetProperty, pnGetProperty);
  napi_set_property(env, proto, pnGetProperty, fnGetProperty);

  napi_value fnIterator = napi_create_function(env, Database::Iterator);
  napi_propertyname pnIterator = napi_property_name(env, "iterator");
  napi_set_function_name(env, fnIterator, pnIterator);
  napi_set_property(env, proto, pnIterator, fnIterator);

  database_constructor = napi_create_persistent(env, ctor);
}

void Database::Destructor (void* obj) {
  Database* db = static_cast<Database*>(obj);
  delete db;
}

NAPI_METHOD(Database::New) {
  napi_value args[1];
  napi_get_cb_args(env, info, args, 1);
  napi_value _this = napi_get_cb_this(env, info);

  Database* obj = new Database(args[0]);

  napi_wrap(env, _this, obj, Database::Destructor, nullptr);

  napi_set_return_value(env, info, _this);
}

napi_value Database::NewInstance (napi_env env, napi_value location) {
  Napi::EscapableHandleScope scope(env);

  napi_value instance;

  napi_value constructorHandle = napi_get_persistent_value(env, database_constructor);

  napi_value argv[] = { location };
  instance = napi_new_instance(env, constructorHandle, 1, argv);

  return scope.Escape(instance);
}

NAPI_METHOD(Database::Open) {
  LD_METHOD_SETUP_COMMON(open, 0, 1)

  bool createIfMissing = BooleanOptionValue(env, optionsObj, "createIfMissing", true);
  bool errorIfExists = BooleanOptionValue(env, optionsObj, "errorIfExists");
  bool compression = BooleanOptionValue(env, optionsObj, "compression", true);

  uint32_t cacheSize = UInt32OptionValue(env, optionsObj, "cacheSize", 8 << 20);
  uint32_t writeBufferSize = UInt32OptionValue(
      env
    , optionsObj
    , "writeBufferSize"
    , 4 << 20
  );
  uint32_t blockSize = UInt32OptionValue(env, optionsObj, "blockSize", 4096);
  uint32_t maxOpenFiles = UInt32OptionValue(env, optionsObj, "maxOpenFiles", 1000);
  uint32_t blockRestartInterval = UInt32OptionValue(
      env
    , optionsObj
    , "blockRestartInterval"
    , 16
  );

  database->blockCache = leveldb::NewLRUCache(cacheSize);
  database->filterPolicy = leveldb::NewBloomFilterPolicy(10);

  OpenWorker* worker = new OpenWorker(
      database
    , callback
    , database->blockCache
    , database->filterPolicy
    , createIfMissing
    , errorIfExists
    , compression
    , writeBufferSize
    , blockSize
    , maxOpenFiles
    , blockRestartInterval
  );
  // persist to prevent accidental GC
  worker->SaveToPersistent("database", _this);
  Napi::AsyncQueueWorker(worker);
}

// for an empty callback to iterator.end()
NAPI_METHOD(EmptyMethod) {
}

NAPI_METHOD(Database::Close) {
  LD_METHOD_SETUP_COMMON_ONEARG(close)

  CloseWorker* worker = new CloseWorker(
      database
    , callback
  );
  // persist to prevent accidental GC
  worker->SaveToPersistent("database", _this);

  if (!database->iterators.empty()) {
    // yikes, we still have iterators open! naughty naughty.
    // we have to queue up a CloseWorker and manually close each of them.
    // the CloseWorker will be invoked once they are all cleaned up
    database->pendingCloseWorker = worker;

    for (
        std::map< uint32_t, leveldown::Iterator * >::iterator it
            = database->iterators.begin()
      ; it != database->iterators.end()
      ; ++it) {

        // for each iterator still open, first check if it's already in
        // the process of ending (ended==true means an async End() is
        // in progress), if not, then we call End() with an empty callback
        // function and wait for it to hit ReleaseIterator() where our
        // CloseWorker will be invoked

        leveldown::Iterator *iterator = it->second;

        napi_value iteratorHandle = napi_get_persistent_value(env, iterator->handle);

        if (!iterator->ended) {
          napi_propertyname pnEnd = napi_property_name(env, "end");
          napi_value end = napi_get_property(env, iteratorHandle, pnEnd);
          napi_value argv[] = {
              napi_create_function(env, EmptyMethod) // empty callback
          };
          napi_make_callback(
              env
            , iteratorHandle
            , end
            , 1
            , argv
          );
        }
    }
  } else {
    Napi::AsyncQueueWorker(worker);
  }
}

NAPI_METHOD(Database::Put) {
  LD_METHOD_SETUP_COMMON(put, 2, 3)

  v8::Local<v8::Object> keyHandle = V8LocalValue(args[0]).As<v8::Object>();
  v8::Local<v8::Object> valueHandle = V8LocalValue(args[1]).As<v8::Object>();
  LD_STRING_OR_BUFFER_TO_SLICE(key, keyHandle, key);
  LD_STRING_OR_BUFFER_TO_SLICE(value, valueHandle, value);

  bool sync = BooleanOptionValue(env, optionsObj, "sync");

  WriteWorker* worker  = new WriteWorker(
      database
    , callback
    , key
    , value
    , sync
    , keyHandle
    , valueHandle
  );

  // persist to prevent accidental GC
  worker->SaveToPersistent("database", _this);
  Napi::AsyncQueueWorker(worker);
}

NAPI_METHOD(Database::Get) {
  LD_METHOD_SETUP_COMMON(get, 1, 2)

  v8::Local<v8::Object> keyHandle = V8LocalValue(args[0]).As<v8::Object>();
  LD_STRING_OR_BUFFER_TO_SLICE(key, keyHandle, key);

  bool asBuffer = BooleanOptionValue(env, optionsObj, "asBuffer", true);
  bool fillCache = BooleanOptionValue(env, optionsObj, "fillCache", true);

  ReadWorker* worker = new ReadWorker(
      database
    , callback
    , key
    , asBuffer
    , fillCache
    , keyHandle
  );
  // persist to prevent accidental GC
  worker->SaveToPersistent("database", _this);
  Napi::AsyncQueueWorker(worker);
}

NAPI_METHOD(Database::Delete) {
  LD_METHOD_SETUP_COMMON(del, 1, 2)

  v8::Local<v8::Object> keyHandle = V8LocalValue(args[0]).As<v8::Object>();
  LD_STRING_OR_BUFFER_TO_SLICE(key, keyHandle, key);

  bool sync = BooleanOptionValue(env, optionsObj, "sync");

  DeleteWorker* worker = new DeleteWorker(
      database
    , callback
    , key
    , sync
    , keyHandle
  );
  // persist to prevent accidental GC
  worker->SaveToPersistent("database", _this);
  Napi::AsyncQueueWorker(worker);
}

NAPI_METHOD(Database::Batch) {
  {
    napi_value args[1];
    napi_get_cb_args(env, info, args, 1);
    int argsLength = napi_get_cb_args_length(env, info);
    if ((argsLength == 0 || argsLength == 1) && !V8LocalValue(args[0])->IsArray()) {
      napi_value optionsObj = nullptr;
      if (argsLength > 0 && napi_get_type_of_value(env, optionsObj) == napi_object) {
        optionsObj = args[0];
      }
      napi_value _this = napi_get_cb_this(env, info);
      napi_set_return_value(env, info, Batch::NewInstance(env, _this, optionsObj));
      return;
    }
  }

  LD_METHOD_SETUP_COMMON(batch, 1, 2);

  bool sync = BooleanOptionValue(env, optionsObj, "sync");

  v8::Local<v8::Array> array = v8::Local<v8::Array>::Cast(V8LocalValue(args[0]));

  leveldb::WriteBatch* batch = new leveldb::WriteBatch();
  bool hasData = false;

  for (unsigned int i = 0; i < array->Length(); i++) {
    if (!array->Get(i)->IsObject())
      continue;

    v8::Local<v8::Object> obj = v8::Local<v8::Object>::Cast(array->Get(i));
    v8::Local<v8::Value> keyBuffer = obj->Get(Nan::New("key").ToLocalChecked());
    v8::Local<v8::Value> type = obj->Get(Nan::New("type").ToLocalChecked());

    if (type->StrictEquals(Nan::New("del").ToLocalChecked())) {
      LD_STRING_OR_BUFFER_TO_SLICE(key, keyBuffer, key)

      batch->Delete(key);
      if (!hasData)
        hasData = true;

      DisposeStringOrBufferFromSlice(keyBuffer, key);
    } else if (type->StrictEquals(Nan::New("put").ToLocalChecked())) {
      v8::Local<v8::Value> valueBuffer = obj->Get(Nan::New("value").ToLocalChecked());

      LD_STRING_OR_BUFFER_TO_SLICE(key, keyBuffer, key)
      LD_STRING_OR_BUFFER_TO_SLICE(value, valueBuffer, value)
      batch->Put(key, value);
      if (!hasData)
        hasData = true;

      DisposeStringOrBufferFromSlice(keyBuffer, key);
      DisposeStringOrBufferFromSlice(valueBuffer, value);
    }
  }

  // don't allow an empty batch through
  if (hasData) {
    BatchWorker* worker = new BatchWorker(
        database
      , callback
      , batch
      , sync
    );
    // persist to prevent accidental GC
    worker->SaveToPersistent("database", _this);
    Napi::AsyncQueueWorker(worker);
  } else {
    LD_RUN_CALLBACK(callback, 0, NULL);
  }
}

NAPI_METHOD(Database::ApproximateSize) {
  LD_METHOD_SETUP_COMMON(approximateSize, -1, 2)

  v8::Local<v8::Object> startHandle = V8LocalValue(args[0]).As<v8::Object>();
  v8::Local<v8::Object> endHandle = V8LocalValue(args[1]).As<v8::Object>();

  LD_STRING_OR_BUFFER_TO_SLICE(start, startHandle, start)
  LD_STRING_OR_BUFFER_TO_SLICE(end, endHandle, end)

  ApproximateSizeWorker* worker  = new ApproximateSizeWorker(
      database
    , callback
    , start
    , end
    , startHandle
    , endHandle
  );
  // persist to prevent accidental GC
  worker->SaveToPersistent("database", _this);
  Napi::AsyncQueueWorker(worker);
}

NAPI_METHOD(Database::GetProperty) {
  napi_value args[1];
  napi_get_cb_args(env, info, args, 1);
  napi_value _this = napi_get_cb_this(env, info);

  v8::Local<v8::Value> propertyHandle = V8LocalValue(args[0]).As<v8::Object>();

  LD_STRING_OR_BUFFER_TO_SLICE(property, propertyHandle, property)

  
  leveldown::Database* database =
      static_cast<leveldown::Database*>(napi_unwrap(env, _this));

  std::string* value = new std::string();
  database->GetPropertyFromDatabase(property, value);
  napi_value returnValue = napi_create_string_with_length(env, value->c_str(), value->length());
  delete value;
  delete[] property.data();

  napi_set_return_value(env, info, returnValue);
}

NAPI_METHOD(Database::Iterator) {
  napi_value args[1];
  napi_get_cb_args(env, info, args, 1);
  int argsLength = napi_get_cb_args_length(env, info);
  napi_value _this = napi_get_cb_this(env, info);

  Database* database = static_cast<leveldown::Database*>(napi_unwrap(env, _this));

  napi_value optionsObj = nullptr;
  if (argsLength > 0 && napi_get_type_of_value(env, args[0]) == napi_object) {
    optionsObj = args[0];
  }

  // each iterator gets a unique id for this Database, so we can
  // easily store & lookup on our `iterators` map
  uint32_t id = database->currentIteratorId++;
  v8::TryCatch try_catch;
  napi_value iteratorHandle = Iterator::NewInstance(
      env
    , _this
    , napi_create_number(env, id)
    , optionsObj
  );
  if (try_catch.HasCaught()) {
    // NB: node::FatalException can segfault here if there is no room on stack.
    return Nan::ThrowError("Fatal Error in Database::Iterator!");
  }

  leveldown::Iterator *iterator =
      static_cast<leveldown::Iterator*>(napi_unwrap(env, iteratorHandle));

  database->iterators[id] = iterator;

  // register our iterator
  /*
  v8::Local<v8::Object> obj = Nan::New<v8::Object>();
  obj->Set(Nan::New("iterator"), iteratorHandle);
  Nan::Persistent<v8::Object> persistent;
  persistent.Reset(nan_isolate, obj);
  database->iterators.insert(std::pair< uint32_t, Nan::Persistent<v8::Object> & >
      (id, persistent));
  */

  napi_set_return_value(env, info, iteratorHandle);
}


} // namespace leveldown
