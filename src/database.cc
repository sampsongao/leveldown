/* Copyright (c) 2012-2016 LevelDOWN contributors
 * See list at <https://github.com/level/leveldown#contributing>
 * MIT License <https://github.com/level/leveldown/blob/master/LICENSE.md>
 */

#include <napi.h>

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

static napi_ref database_constructor;

Database::Database (napi_env env, napi_value from)
  : location(std::move(Napi::String(env, from)))
  , db(NULL)
  , currentIteratorId(0)
  , pendingCloseWorker(NULL)
  , blockCache(NULL)
  , filterPolicy(NULL) {};

Database::~Database () {
  if (db != NULL)
    delete db;
};

/* Calls from worker threads, NO V8 HERE *****************************/

leveldb::Status Database::OpenDatabase (
        leveldb::Options* options
    ) {
  return leveldb::DB::Open(*options, location, &db);
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
    ((AsyncWorker*)pendingCloseWorker)->Queue();
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

napi_value LevelDOWN (napi_env env, napi_callback_info info) {
  napi_value args[1];
  size_t argc = 1;
  CHECK_NAPI_RESULT(napi_get_cb_info(env, info, &argc, args, nullptr, nullptr));

  napi_value location = args[0];
  return Database::NewInstance(env, location);
}

void Database::Init (napi_env env) {
  napi_property_descriptor methods [] = {
    { "open", nullptr, Database::Open },
    { "close", nullptr, Database::Close },
    { "put", nullptr, Database::Put },
    { "get", nullptr, Database::Get },
    { "del", nullptr, Database::Delete },
    { "batch", nullptr, Database::Batch },
    { "approximateSize", nullptr, Database::ApproximateSize },
    { "getProperty", nullptr, Database::GetProperty },
    { "iterator", nullptr, Database::Iterator },
  };

  napi_value ctor;
  CHECK_NAPI_RESULT(napi_define_class(env, "Database", Database::New, nullptr, 9, methods, &ctor));
  CHECK_NAPI_RESULT(napi_create_reference(env, ctor, 1, &database_constructor));
}

void Database::Destructor(napi_env env, void* obj, void* hint) {
  Database* db = static_cast<Database*>(obj);
  delete db;
}

NAPI_METHOD(Database::New) {
  size_t argc = 1;
  napi_value args[1];
  napi_value _this;
  CHECK_NAPI_RESULT(napi_get_cb_info(env, info, &argc, args, &_this, nullptr));

  Database* obj = new Database(env, args[0]);

  CHECK_NAPI_RESULT(napi_wrap(env, _this, obj, Database::Destructor, nullptr, nullptr));
  return _this;
}

napi_value Database::NewInstance (napi_env env, napi_value location) {
  Napi::EscapableHandleScope scope(env);

  napi_value constructorHandle;
  CHECK_NAPI_RESULT(napi_get_reference_value(env, database_constructor, &constructorHandle));

  napi_value argv[] = { location };
  napi_value instance;
  CHECK_NAPI_RESULT(napi_new_instance(env, constructorHandle, 1, argv, &instance));

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
    , env
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
  worker->Receiver().Set("database", _this);
  worker->Queue();

  return nullptr;
}

// for an empty callback to iterator.end()
NAPI_METHOD(EmptyMethod) {
  return nullptr;
}

NAPI_METHOD(Database::Close) {
  LD_METHOD_SETUP_COMMON_ONEARG(close)

  CloseWorker* worker = new CloseWorker(
      database
    , env
    , callback
  );
  // persist to prevent accidental GC
  worker->Receiver().Set("database", _this);

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

        napi_value iteratorHandle;
        CHECK_NAPI_RESULT(napi_get_reference_value(env, iterator->handle, &iteratorHandle));
        if (iteratorHandle != nullptr) {
          if (!iterator->ended) {
            napi_value end;
            CHECK_NAPI_RESULT(napi_get_named_property(env, iteratorHandle, "end", &end));
            napi_value emptyFn;
            CHECK_NAPI_RESULT(napi_create_function(env, "end", EmptyMethod, nullptr, &emptyFn));
            napi_value argv [] = {
                emptyFn // empty callback
            };
            napi_value unused;
            CHECK_NAPI_RESULT(napi_make_callback(
              env
              , iteratorHandle
              , end
              , 1
              , argv
              , &unused
            ));
          }
        }
        else {
          // todo(ianhall): fail?
        }
    }
  } else {
    worker->Queue();
  }

  return nullptr;
}

NAPI_METHOD(Database::Put) {
  LD_METHOD_SETUP_COMMON(put, 2, 3)

  napi_value keyHandle = args[0];
  napi_value valueHandle = args[1];
  LD_STRING_OR_BUFFER_TO_SLICE(key, keyHandle, key);
  LD_STRING_OR_BUFFER_TO_SLICE(value, valueHandle, value);

  bool sync = BooleanOptionValue(env, optionsObj, "sync");

  WriteWorker* worker  = new WriteWorker(
      database
    , env
    , callback
    , key
    , value
    , sync
    , keyHandle
    , valueHandle
  );

  // persist to prevent accidental GC
  worker->Receiver().Set("database", _this);
  worker->Queue();
  return nullptr;
}

NAPI_METHOD(Database::Get) {
  LD_METHOD_SETUP_COMMON(get, 1, 2)

  napi_value keyHandle = args[0];
  LD_STRING_OR_BUFFER_TO_SLICE(key, keyHandle, key);

  bool asBuffer = BooleanOptionValue(env, optionsObj, "asBuffer", true);
  bool fillCache = BooleanOptionValue(env, optionsObj, "fillCache", true);

  ReadWorker* worker = new ReadWorker(
      database
    , env
    , callback
    , key
    , asBuffer
    , fillCache
    , keyHandle
  );
  // persist to prevent accidental GC
  worker->Receiver().Set("database", _this);
  worker->Queue();
  return nullptr;
}

NAPI_METHOD(Database::Delete) {
  LD_METHOD_SETUP_COMMON(del, 1, 2)

  napi_value keyHandle = args[0];
  LD_STRING_OR_BUFFER_TO_SLICE(key, keyHandle, key);

  bool sync = BooleanOptionValue(env, optionsObj, "sync");

  DeleteWorker* worker = new DeleteWorker(
      database
    , env
    , callback
    , key
    , sync
    , keyHandle
  );
  // persist to prevent accidental GC
  worker->Receiver().Set("database", _this);
  worker->Queue();
  return nullptr;
}

NAPI_METHOD(Database::Batch) {
  {
    size_t argc = 1;
    napi_value args[1];
    napi_value _this;
    CHECK_NAPI_RESULT(napi_get_cb_info(env, info, &argc, args, &_this, nullptr));
    if (argc == 0 || argc == 1) {
      bool isArray = false;
      if (argc > 0) {
        CHECK_NAPI_RESULT(napi_is_array(env, args[0], &isArray));
      }
      if (!isArray) {
        napi_value optionsObj = nullptr;
        if (argc > 0) {
          napi_valuetype t;
          CHECK_NAPI_RESULT(napi_typeof(env, args[0], &t));
          if (t == napi_object) {
            optionsObj = args[0];
          }
        }
        return Batch::NewInstance(env, _this, optionsObj);
      }
    }
  }

  LD_METHOD_SETUP_COMMON(batch, 1, 2);

  bool sync = BooleanOptionValue(env, optionsObj, "sync");

  napi_value array = args[0];

  leveldb::WriteBatch* batch = new leveldb::WriteBatch();
  bool hasData = false;

  uint32_t length;
  CHECK_NAPI_RESULT(napi_get_array_length(env, array, &length));

  for (unsigned int i = 0; i < length; i++) {
    napi_value obj;
    CHECK_NAPI_RESULT(napi_get_element(env, array, i, &obj));

    napi_valuetype t;
    CHECK_NAPI_RESULT(napi_typeof(env, obj, &t));
    if (t != napi_object)
      continue;

    napi_value keyBuffer;
    CHECK_NAPI_RESULT(napi_get_named_property(env, obj, "key", &keyBuffer));
    napi_value type;
    CHECK_NAPI_RESULT(napi_get_named_property(env, obj, "type", &type));
    napi_value delStr;
    CHECK_NAPI_RESULT(napi_create_string_utf8(env, "del", strlen("del"), &delStr));
    bool r;
    CHECK_NAPI_RESULT(napi_strict_equals(env, type, delStr, &r));

    if (r) {
      LD_STRING_OR_BUFFER_TO_SLICE(key, keyBuffer, key)

      batch->Delete(key);
      if (!hasData)
        hasData = true;

      DisposeStringOrBufferFromSlice(env, keyBuffer, key);
    } else {
      napi_value putStr;
      CHECK_NAPI_RESULT(napi_create_string_utf8(env, "put", strlen("put"), &putStr));
      CHECK_NAPI_RESULT(napi_strict_equals(env, type, putStr, &r));

      if (r) {
        napi_value valueBuffer;
        CHECK_NAPI_RESULT(napi_get_named_property(env, obj, "value", &valueBuffer));

        LD_STRING_OR_BUFFER_TO_SLICE(key, keyBuffer, key)
        LD_STRING_OR_BUFFER_TO_SLICE(value, valueBuffer, value)
        batch->Put(key, value);
        if (!hasData)
          hasData = true;

        DisposeStringOrBufferFromSlice(env, keyBuffer, key);
        DisposeStringOrBufferFromSlice(env, valueBuffer, value);
      }
    }
  }

  // don't allow an empty batch through
  if (hasData) {
    BatchWorker* worker = new BatchWorker(
        database
      , env
      , callback
      , batch
      , sync
    );
    // persist to prevent accidental GC
    worker->Receiver().Set("database", _this);
    worker->Queue();
  } else {
    LD_RUN_CALLBACK(callback, 0, NULL);
  }
  return nullptr;
}

NAPI_METHOD(Database::ApproximateSize) {
  LD_METHOD_SETUP_COMMON(approximateSize, -1, 2)

  napi_value startHandle = args[0];
  napi_value endHandle = args[1];

  LD_STRING_OR_BUFFER_TO_SLICE(start, startHandle, start)
  LD_STRING_OR_BUFFER_TO_SLICE(end, endHandle, end)

  ApproximateSizeWorker* worker  = new ApproximateSizeWorker(
      database
    , env
    , callback
    , start
    , end
    , startHandle
    , endHandle
  );
  // persist to prevent accidental GC
  worker->Receiver().Set("database", _this);
  worker->Queue();
  return nullptr;
}

NAPI_METHOD(Database::GetProperty) {
  size_t argc = 1;
  napi_value args[1];
  napi_value _this;
  CHECK_NAPI_RESULT(napi_get_cb_info(env, info, &argc, args, &_this, nullptr));

  napi_value propertyHandle = args[0];

  LD_STRING_OR_BUFFER_TO_SLICE(property, propertyHandle, property)

  void* unwrapped;
  CHECK_NAPI_RESULT(napi_unwrap(env, _this, &unwrapped));
  leveldown::Database* database =
      static_cast<leveldown::Database*>(unwrapped);

  std::string* value = new std::string();
  database->GetPropertyFromDatabase(property, value);
  napi_value returnValue;
  CHECK_NAPI_RESULT(napi_create_string_utf8(env, value->c_str(), value->length(), &returnValue));

  delete value;
  delete[] property.data();

  return returnValue;
}

NAPI_METHOD(Database::Iterator) {
  size_t argc = 1;
  napi_value args[1];
  napi_value _this;
  CHECK_NAPI_RESULT(napi_get_cb_info(env, info, &argc, args, &_this, nullptr));

  void* unwrapped;
  CHECK_NAPI_RESULT(napi_unwrap(env, _this, &unwrapped));
  leveldown::Database* database =
      static_cast<leveldown::Database*>(unwrapped);

  napi_value optionsObj = nullptr;
  if (argc > 0) {
    napi_valuetype t;
    CHECK_NAPI_RESULT(napi_typeof(env, args[0], &t));

    if (t == napi_object) {
      optionsObj = args[0];
    }
  }

  // each iterator gets a unique id for this Database, so we can
  // easily store & lookup on our `iterators` map
  uint32_t id = database->currentIteratorId++;

  napi_value iteratorHandle;
  napi_value idVal;
  CHECK_NAPI_RESULT(napi_create_double(env, id, &idVal));

  iteratorHandle = Iterator::NewInstance(
    env
    , _this
    , idVal
    , optionsObj
  );

  bool r;
  CHECK_NAPI_RESULT(napi_is_exception_pending(env, &r));
  if (r) {
    // NB: node::FatalException can segfault here if there is no room on stack.
    CHECK_NAPI_RESULT(napi_throw_error(env, nullptr, "Fatal Error in Database::Iterator!"));
  }

  CHECK_NAPI_RESULT(napi_unwrap(env, iteratorHandle, &unwrapped));
  leveldown::Iterator *iterator = static_cast<leveldown::Iterator*>(unwrapped);

  database->iterators[id] = iterator;

  return iteratorHandle;
}
} // namespace leveldown
