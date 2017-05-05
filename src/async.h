/* Copyright (c) 2012-2016 LevelDOWN contributors
 * See list at <https://github.com/level/leveldown#contributing>
 * MIT License <https://github.com/level/leveldown/blob/master/LICENSE.md>
 */

#ifndef LD_ASYNC_H
#define LD_ASYNC_H

#include <napi.h>
#include "database.h"

namespace leveldown {

class Database;

/* abstract */ class AsyncWorker : public Napi::AsyncWorker {
public:
  AsyncWorker (
      leveldown::Database* database
    , napi_env env
    , napi_value callback
  ) : Napi::AsyncWorker(Napi::Function(env, callback)), database(database) { }

protected:
  void SetStatus(leveldb::Status status) {
    this->status = status;
    if (!status.ok())
      SetError(status.ToString());
  }
  Database* database;
private:
  leveldb::Status status;
};

} // namespace leveldown

#endif
