/* Copyright (c) 2012-2016 LevelDOWN contributors
 * See list at <https://github.com/level/leveldown#contributing>
 * MIT License <https://github.com/level/leveldown/blob/master/LICENSE.md>
 */

#include <node.h>
#include <node_jsvmapi.h>

#include "leveldown.h"
#include "database.h"
#include "iterator.h"
#include "batch.h"
#include "leveldown_async.h"

namespace leveldown {

void DestroyDB (napi_env env, napi_func_cb_info info) {
  napi_value args[2];
  napi_get_cb_args(env, info, args, 2);

  Napi::Utf8String* location = new Napi::Utf8String(args[0]);

  napi_value callback = args[1];

  DestroyWorker* worker = new DestroyWorker(
      location
    , callback
  );

  Nan::AsyncQueueWorker(worker);

  napi_set_return_value(env, info, napi_get_undefined_(env));
}

void RepairDB (napi_env env, napi_func_cb_info info) {
  napi_value args[2];
  napi_get_cb_args(env, info, args, 2);

  Napi::Utf8String* location = new Napi::Utf8String(args[0]);

  napi_value callback = args[1];

  RepairWorker* worker = new RepairWorker(
      location
    , callback
  );

  Nan::AsyncQueueWorker(worker);

  napi_set_return_value(env, info, napi_get_undefined_(env));
}

void Init(napi_env env, napi_value target, napi_value module) {
  Database::Init(env);
  leveldown::Iterator::Init(env);
  leveldown::Batch::Init(env);

  napi_propertyname nameDestroy = napi_property_name(env, "destroy");
  napi_propertyname nameRepair = napi_property_name(env, "repair");
  napi_propertyname nameLeveldown = napi_property_name(env, "leveldown");

  napi_value leveldown = napi_create_function(env, LevelDOWN);

  napi_set_property(env, leveldown, nameDestroy, napi_create_function(env, DestroyDB));
  napi_set_property(env, leveldown, nameRepair, napi_create_function(env, RepairDB));

  napi_set_property(env, target, nameLeveldown, leveldown);
}

NODE_MODULE_ABI(leveldown, Init)

} // namespace leveldown
