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

void Init(napi_env env, napi_value target, napi_value module, void* priv) {
  Database::Init(env);
  leveldown::Iterator::Init(env);
  leveldown::Batch::Init(env);

  napi_value leveldown;
  CHECK_NAPI_RESULT(napi_create_function(env, "leveldown", LevelDOWN, nullptr, &leveldown));
  napi_value functionDestroy;
  CHECK_NAPI_RESULT(napi_create_function(env, "destroy", DestroyDB, nullptr, &functionDestroy));
  napi_value functionRepair;
  CHECK_NAPI_RESULT(napi_create_function(env, "repair", RepairDB, nullptr, &functionRepair));

  CHECK_NAPI_RESULT(napi_set_named_property(env, leveldown, "destroy", functionDestroy));
  CHECK_NAPI_RESULT(napi_set_named_property(env, leveldown, "repair", functionRepair));
  CHECK_NAPI_RESULT(napi_set_named_property(env, target, "leveldown", leveldown));
}

NAPI_MODULE(leveldown, Init)

} // namespace leveldown
