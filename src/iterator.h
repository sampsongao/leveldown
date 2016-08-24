/* Copyright (c) 2012-2016 LevelDOWN contributors
 * See list at <https://github.com/level/leveldown#contributing>
 * MIT License <https://github.com/level/leveldown/blob/master/LICENSE.md>
 */

#ifndef LD_ITERATOR_H
#define LD_ITERATOR_H

#include <node.h>
#include <vector>
#include <nan.h>

#include "leveldown.h"
#include "database.h"
#include "async.h"

namespace leveldown {

class Database;
class AsyncWorker;

class Iterator {
public:
  static void Init (napi_env env);
  static napi_value NewInstance (
      napi_env env
    , napi_value database
    , napi_value id
    , napi_value optionsObj
  );

  Iterator (
      Database* database
    , uint32_t id
    , leveldb::Slice* start
    , std::string* end
    , bool reverse
    , bool keys
    , bool values
    , int limit
    , std::string* lt
    , std::string* lte
    , std::string* gt
    , std::string* gte
    , bool fillCache
    , bool keyAsBuffer
    , bool valueAsBuffer
    , size_t highWaterMark
  );

  ~Iterator ();

  bool IteratorNext (std::vector<std::pair<std::string, std::string> >& result);
  leveldb::Status IteratorStatus ();
  void IteratorEnd ();
  void Release ();

private:
  Database* database;
  uint32_t id;
  leveldb::Iterator* dbIterator;
  leveldb::ReadOptions* options;
  leveldb::Slice* start;
  std::string* end;
  bool seeking;
  bool reverse;
  bool keys;
  bool values;
  int limit;
  std::string* lt;
  std::string* lte;
  std::string* gt;
  std::string* gte;
  int count;
  size_t highWaterMark;

public:
  napi_weakref handle;
  bool keyAsBuffer;
  bool valueAsBuffer;
  bool nexting;
  bool ended;
  AsyncWorker* endWorker;

private:
  bool Read (std::string& key, std::string& value);
  bool GetIterator ();

  static void Destructor(void* obj);

  static NAPI_METHOD(New);
  static NAPI_METHOD(Seek);
  static NAPI_METHOD(Next);
  static NAPI_METHOD(End);
};

} // namespace leveldown

#endif
