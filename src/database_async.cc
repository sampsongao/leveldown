/* Copyright (c) 2012-2016 LevelDOWN contributors
 * See list at <https://github.com/level/leveldown#contributing>
 * MIT License <https://github.com/level/leveldown/blob/master/LICENSE.md>
 */

#include <napi.h>

#include <leveldb/write_batch.h>
#include <leveldb/filter_policy.h>

#include "database.h"
#include "leveldown.h"
#include "async.h"
#include "database_async.h"

namespace leveldown {

/** OPEN WORKER **/

OpenWorker::OpenWorker (
    Database *database
  , napi_env env
  , napi_value callback
  , leveldb::Cache* blockCache
  , const leveldb::FilterPolicy* filterPolicy
  , bool createIfMissing
  , bool errorIfExists
  , bool compression
  , uint32_t writeBufferSize
  , uint32_t blockSize
  , uint32_t maxOpenFiles
  , uint32_t blockRestartInterval
) : AsyncWorker(database, env, callback)
{
  options = new leveldb::Options();
  options->block_cache            = blockCache;
  options->filter_policy          = filterPolicy;
  options->create_if_missing      = createIfMissing;
  options->error_if_exists        = errorIfExists;
  options->compression            = compression
      ? leveldb::kSnappyCompression
      : leveldb::kNoCompression;
  options->write_buffer_size      = writeBufferSize;
  options->block_size             = blockSize;
  options->max_open_files         = maxOpenFiles;
  options->block_restart_interval = blockRestartInterval;
};

OpenWorker::~OpenWorker () {
  delete options;
}

void OpenWorker::Execute () {
  SetStatus(database->OpenDatabase(options));
}

/** CLOSE WORKER **/

CloseWorker::CloseWorker (
    Database *database
  , napi_env env
  , napi_value callback
) : AsyncWorker(database, env, callback)
{};

CloseWorker::~CloseWorker () {}

void CloseWorker::Execute () {
  database->CloseDatabase();
}

void CloseWorker::WorkComplete () {
  Napi::HandleScope scope(Env());
  OnOK();
  _callback.Reset();
}

/** IO WORKER (abstract) **/

IOWorker::IOWorker (
    Database *database
  , napi_env env
  , napi_value callback
  , leveldb::Slice key
  , napi_value keyHandle
) : AsyncWorker(database, env, callback)
  , key(key)
{
  Napi::HandleScope scope(env);

  _persistent.Set("key", keyHandle);
};

IOWorker::~IOWorker () {}

void IOWorker::WorkComplete () {
  Napi::HandleScope scope(Env());

  DisposeStringOrBufferFromSlice(Env(), _persistent.Get("key"), key);
  AsyncWorker::WorkComplete();
}

/** READ WORKER **/

ReadWorker::ReadWorker (
    Database *database
  , napi_env env
  , napi_value callback
  , leveldb::Slice key
  , bool asBuffer
  , bool fillCache
  , napi_value keyHandle
) : IOWorker(database, env, callback, key, keyHandle)
  , asBuffer(asBuffer)
{
  Napi::HandleScope scope(env);

  options = new leveldb::ReadOptions();
  options->fill_cache = fillCache;
  _persistent.Set("key", keyHandle);
};

ReadWorker::~ReadWorker () {
  delete options;
}

void ReadWorker::Execute () {
  SetStatus(database->GetFromDatabase(options, key, value));
}

void ReadWorker::OnOK () {
  Napi::HandleScope scope(Env());

  napi_value returnValue;
  if (asBuffer) {
    //TODO: could use NewBuffer if we carefully manage the lifecycle of `value`
    //and avoid an an extra allocation. We'd have to clean up properly when not OK
    //and let the new Buffer manage the data when OK
    CHECK_NAPI_RESULT(napi_create_buffer_copy(Env(), value.data(), value.size(), &returnValue));
  } else {
    CHECK_NAPI_RESULT(napi_create_string_utf8(Env(), (char*)value.data(), value.size(), &returnValue));
  }

  napi_value nullVal;
  CHECK_NAPI_RESULT(napi_get_null(Env(), &nullVal));

  napi_value globalVal;
  CHECK_NAPI_RESULT(napi_get_global(Env(), &globalVal));

  _callback.Call(globalVal, {
    nullVal,
    returnValue,
  });
}

/** DELETE WORKER **/

DeleteWorker::DeleteWorker (
    Database *database
  , napi_env env
  , napi_value callback
  , leveldb::Slice key
  , bool sync
  , napi_value keyHandle
) : IOWorker(database, env, callback, key, keyHandle)
{
  Napi::HandleScope scope(env);

  options = new leveldb::WriteOptions();
  options->sync = sync;
  _persistent.Set("key", keyHandle);
};

DeleteWorker::~DeleteWorker () {
  delete options;
}

void DeleteWorker::Execute () {
  SetStatus(database->DeleteFromDatabase(options, key));
}

/** WRITE WORKER **/

WriteWorker::WriteWorker (
    Database *database
  , napi_env env
  , napi_value callback
  , leveldb::Slice key
  , leveldb::Slice value
  , bool sync
  , napi_value keyHandle
  , napi_value valueHandle
) : DeleteWorker(database, env, callback, key, sync, keyHandle)
  , value(value)
{
  Napi::HandleScope scope(env);

  _persistent.Set("value", valueHandle);
};

WriteWorker::~WriteWorker () { }

void WriteWorker::Execute () {
  SetStatus(database->PutToDatabase(options, key, value));
}

void WriteWorker::WorkComplete () {
  Napi::HandleScope scope(Env());

  DisposeStringOrBufferFromSlice(Env(), _persistent.Get("value"), value);
  IOWorker::WorkComplete();
}

/** BATCH WORKER **/

BatchWorker::BatchWorker (
    Database *database
  , napi_env env
  , napi_value callback
  , leveldb::WriteBatch* batch
  , bool sync
) : AsyncWorker(database, env, callback)
  , batch(batch)
{
  options = new leveldb::WriteOptions();
  options->sync = sync;
};

BatchWorker::~BatchWorker () {
  delete batch;
  delete options;
}

void BatchWorker::Execute () {
  SetStatus(database->WriteBatchToDatabase(options, batch));
}

/** APPROXIMATE SIZE WORKER **/

ApproximateSizeWorker::ApproximateSizeWorker (
    Database *database
  , napi_env env
  , napi_value callback
  , leveldb::Slice start
  , leveldb::Slice end
  , napi_value startHandle
  , napi_value endHandle
) : AsyncWorker(database, env, callback)
  , range(start, end)
{
  Napi::HandleScope scope(env);

  _persistent.Set("start", startHandle);
  _persistent.Set("end", endHandle);
};

ApproximateSizeWorker::~ApproximateSizeWorker () {}

void ApproximateSizeWorker::Execute () {
  size = database->ApproximateSizeFromDatabase(&range);
}

void ApproximateSizeWorker::WorkComplete() {
  Napi::HandleScope scope(Env());

  DisposeStringOrBufferFromSlice(Env(), _persistent.Get("start"), range.start);
  DisposeStringOrBufferFromSlice(Env(), _persistent.Get("end"), range.limit);
  AsyncWorker::WorkComplete();
}

void ApproximateSizeWorker::OnOK () {
  Napi::HandleScope scope(Env());

  napi_value returnValue;
  CHECK_NAPI_RESULT(napi_create_number(Env(), (double)size, &returnValue));

  napi_value nullVal;
  CHECK_NAPI_RESULT(napi_get_null(Env(), &nullVal));

  napi_value globalVal;
  CHECK_NAPI_RESULT(napi_get_global(Env(), &globalVal));

  _callback.Call(globalVal, {
    nullVal,
    returnValue,
  });
}

} // namespace leveldown
