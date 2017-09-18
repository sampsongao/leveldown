/* Copyright (c) 2012-2016 LevelDOWN contributors
 * See list at <https://github.com/level/leveldown#contributing>
 * MIT License <https://github.com/level/leveldown/blob/master/LICENSE.md>
 */

#include <napi.h>

#include "leveldown.h"
#include "database.h"
#include "iterator.h"
#include "batch.h"
#include "leveldown_async.h"

namespace leveldown {

napi_value DestroyDB (napi_env env, napi_callback_info info) {
  size_t argc = 2;
  napi_value args[2];
  CHECK_NAPI_RESULT(napi_get_cb_info(env, info, &argc, args, nullptr, nullptr));

  std::string location = std::move(Napi::String(env, args[0]));

  napi_value callback = args[1];

  DestroyWorker* worker = new DestroyWorker(
      location
    , env
    , callback
  );

  worker->Queue();
  return nullptr;
}

napi_value RepairDB (napi_env env, napi_callback_info info) {
  size_t argc = 2;
  napi_value args[2];
  CHECK_NAPI_RESULT(napi_get_cb_info(env, info, &argc, args, nullptr, nullptr));

  std::string location = std::move(Napi::String(env, args[0]));

  napi_value callback = args[1];

  RepairWorker* worker = new RepairWorker(
      location
    , env
    , callback
  );

  worker->Queue();
  return nullptr;
}

napi_value Init(napi_env env, napi_value exports) {
  Database::Init(env);
  leveldown::Iterator::Init(env);
  leveldown::Batch::Init(env);

  napi_value leveldown;
  CHECK_NAPI_RESULT(napi_create_function(env, "leveldown", -1, LevelDOWN, nullptr, &leveldown));
  napi_value functionDestroy;
  CHECK_NAPI_RESULT(napi_create_function(env, "destroy", -1, DestroyDB, nullptr, &functionDestroy));
  napi_value functionRepair;
  CHECK_NAPI_RESULT(napi_create_function(env, "repair", -1, RepairDB, nullptr, &functionRepair));

  CHECK_NAPI_RESULT(napi_set_named_property(env, leveldown, "destroy", functionDestroy));
  CHECK_NAPI_RESULT(napi_set_named_property(env, leveldown, "repair", functionRepair));
  CHECK_NAPI_RESULT(napi_set_named_property(env, exports, "leveldown", leveldown));

  return exports;
}

NAPI_MODULE(leveldown, Init)

} // namespace leveldown
