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

void DestroyDB (napi_env env, napi_callback_info info) {
  napi_value args[2];
  CHECK_NAPI_RESULT(napi_get_cb_args(env, info, args, 2));

  Napi::Utf8String* location = new Napi::Utf8String(args[0]);

  napi_value callback = args[1];

  DestroyWorker* worker = new DestroyWorker(
      location
    , callback
  );

  Napi::AsyncQueueWorker(worker);

  napi_value undefined;
  CHECK_NAPI_RESULT(napi_get_undefined(env, &undefined));
  CHECK_NAPI_RESULT(napi_set_return_value(env, info, undefined));
}

void RepairDB (napi_env env, napi_callback_info info) {
  napi_value args[2];
  CHECK_NAPI_RESULT(napi_get_cb_args(env, info, args, 2));

  Napi::Utf8String* location = new Napi::Utf8String(args[0]);

  napi_value callback = args[1];

  RepairWorker* worker = new RepairWorker(
      location
    , callback
  );

  Napi::AsyncQueueWorker(worker);

  napi_value undefined;
  CHECK_NAPI_RESULT(napi_get_undefined(env, &undefined));
  CHECK_NAPI_RESULT(napi_set_return_value(env, info, undefined));
}

void Init(napi_env env, napi_value target, napi_value module) {
  Database::Init(env);
  leveldown::Iterator::Init(env);
  leveldown::Batch::Init(env);

  napi_propertyname nameDestroy;
  CHECK_NAPI_RESULT(napi_property_name(env, "destroy", &nameDestroy));
  napi_propertyname nameRepair;
  CHECK_NAPI_RESULT(napi_property_name(env, "repair", &nameRepair));
  napi_propertyname nameLeveldown;
  CHECK_NAPI_RESULT(napi_property_name(env, "leveldown", &nameLeveldown));

  napi_value leveldown;
  CHECK_NAPI_RESULT(napi_create_function(env, LevelDOWN, nullptr, &leveldown));
  napi_value functionDestroy;
  CHECK_NAPI_RESULT(napi_create_function(env, DestroyDB, nullptr, &functionDestroy));
  napi_value functionRepair;
  CHECK_NAPI_RESULT(napi_create_function(env, RepairDB, nullptr, &functionRepair));

  CHECK_NAPI_RESULT(napi_set_property(env, leveldown, nameDestroy, functionDestroy));
  CHECK_NAPI_RESULT(napi_set_property(env, leveldown, nameRepair, functionRepair));
  CHECK_NAPI_RESULT(napi_set_property(env, target, nameLeveldown, leveldown));
}

NODE_MODULE_ABI(leveldown, Init)

} // namespace leveldown
