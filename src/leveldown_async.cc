/* Copyright (c) 2012-2016 LevelDOWN contributors
 * See list at <https://github.com/level/leveldown#contributing>
 * MIT License <https://github.com/level/leveldown/blob/master/LICENSE.md>
 */

#include <leveldb/db.h>

#include "leveldown.h"
#include "leveldown_async.h"

namespace leveldown {

/** DESTROY WORKER **/

DestroyWorker::DestroyWorker (
    std::string location
  , napi_env env
  , napi_value callback
) : AsyncWorker(NULL, env, callback)
  , location(location)
{};

DestroyWorker::~DestroyWorker () {
}

void DestroyWorker::Execute () {
  leveldb::Options options;
  SetStatus(leveldb::DestroyDB(location, options));
}

/** REPAIR WORKER **/

RepairWorker::RepairWorker (
    std::string location
  , napi_env env
  , napi_value callback
) : AsyncWorker(NULL, env, callback)
  , location(location)
{};

RepairWorker::~RepairWorker () {
}

void RepairWorker::Execute () {
  leveldb::Options options;
  SetStatus(leveldb::RepairDB(location, options));
}

} // namespace leveldown
