/* Copyright (c) 2012-2016 LevelDOWN contributors
 * See list at <https://github.com/level/leveldown#contributing>
 * MIT License <https://github.com/level/leveldown/blob/master/LICENSE.md>
 */

#ifndef LD_DATABASE_H
#define LD_DATABASE_H

#include <map>
#include <vector>
#include <node.h>
#include <node_jsvmapi.h>

#include <leveldb/cache.h>
#include <leveldb/db.h>
#include <leveldb/filter_policy.h>
#include <nan.h>

#include "leveldown.h"
#include "iterator.h"

namespace leveldown {

void LevelDOWN (napi_env env, napi_func_cb_info info);

/* TODO (ianhall): This code is unused so not converting it
struct Reference {
  Nan::Persistent<v8::Object> handle;
  leveldb::Slice slice;

  Reference(v8::Local<v8::Value> obj, leveldb::Slice slice) : slice(slice) {
    v8::Local<v8::Object> _obj = Nan::New<v8::Object>();
    _obj->Set(Nan::New("obj").ToLocalChecked(), obj);
    handle.Reset(_obj);
  };
};

static inline void ClearReferences (std::vector<Reference *> *references) {
  for (std::vector<Reference *>::iterator it = references->begin()
      ; it != references->end()
      ; ) {
    DisposeStringOrBufferFromSlice((*it)->handle, (*it)->slice);
    it = references->erase(it);
  }
  delete references;
}
*/

class Database {
public:
  static void Init (napi_env env);
  static napi_value NewInstance (napi_env env, napi_value location);

  leveldb::Status OpenDatabase (leveldb::Options* options);
  leveldb::Status PutToDatabase (
      leveldb::WriteOptions* options
    , leveldb::Slice key
    , leveldb::Slice value
  );
  leveldb::Status GetFromDatabase (
      leveldb::ReadOptions* options
    , leveldb::Slice key
    , std::string& value
  );
  leveldb::Status DeleteFromDatabase (
      leveldb::WriteOptions* options
    , leveldb::Slice key
  );
  leveldb::Status WriteBatchToDatabase (
      leveldb::WriteOptions* options
    , leveldb::WriteBatch* batch
  );
  uint64_t ApproximateSizeFromDatabase (const leveldb::Range* range);
  void GetPropertyFromDatabase (const leveldb::Slice& property, std::string* value);
  leveldb::Iterator* NewIterator (leveldb::ReadOptions* options);
  const leveldb::Snapshot* NewSnapshot ();
  void ReleaseSnapshot (const leveldb::Snapshot* snapshot);
  void CloseDatabase ();
  void ReleaseIterator (uint32_t id);

  Database (napi_value from);
  ~Database ();

private:
  Napi::Utf8String* location;
  leveldb::DB* db;
  uint32_t currentIteratorId;
  void(*pendingCloseWorker);
  leveldb::Cache* blockCache;
  const leveldb::FilterPolicy* filterPolicy;

  std::map< uint32_t, leveldown::Iterator * > iterators;

  static void WriteDoing(uv_work_t *req);
  static void WriteAfter(uv_work_t *req);

  static void Destructor (void* obj);

  static NAPI_METHOD(New);
  static NAPI_METHOD(Open);
  static NAPI_METHOD(Close);
  static NAPI_METHOD(Put);
  static NAPI_METHOD(Delete);
  static NAPI_METHOD(Get);
  static NAPI_METHOD(Batch);
  static NAPI_METHOD(Write);
  static NAPI_METHOD(Iterator);
  static NAPI_METHOD(ApproximateSize);
  static NAPI_METHOD(GetProperty);
};

} // namespace leveldown

#endif
