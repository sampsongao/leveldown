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
    , napi_value callback
  ) : Napi::AsyncWorker(new Napi::Callback(callback)), database(database) { }

protected:
  void SetStatus(leveldb::Status status) {
    this->status = status;
    if (!status.ok())
      SetErrorMessage(status.ToString().c_str());
  }
  Database* database;
private:
  leveldb::Status status;
};

} // namespace leveldown

#endif
