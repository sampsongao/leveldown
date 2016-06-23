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

void DestroyDB (node::js::env env, node::js::FunctionCallbackInfo info) {
  node::js::value args[2];
  node::js::GetCallbackArgs(env, info, args, 2);

  Nan::Utf8String* location = new Nan::Utf8String(node::js::legacy::V8LocalValue(args[0]));

  Nan::Callback* callback = new Nan::Callback(
      v8::Local<v8::Function>::Cast(node::js::legacy::V8LocalValue(args[1])));

  DestroyWorker* worker = new DestroyWorker(
      location
    , callback
  );

  Nan::AsyncQueueWorker(worker);

  node::js::SetReturnValue(env, info, node::js::GetUndefined(env));
}

void RepairDB (node::js::env env, node::js::FunctionCallbackInfo info) {
  node::js::value args[2];
  node::js::GetCallbackArgs(env, info, args, 2);

  Nan::Utf8String* location = new Nan::Utf8String(node::js::legacy::V8LocalValue(args[0]));

  Nan::Callback* callback = new Nan::Callback(
      v8::Local<v8::Function>::Cast(node::js::legacy::V8LocalValue(args[1])));

  RepairWorker* worker = new RepairWorker(
      location
    , callback
  );

  Nan::AsyncQueueWorker(worker);

  node::js::SetReturnValue(env, info, node::js::GetUndefined(env));
}

void NewInit(node::js::env env, node::js::value target, node::js::value module) {
  Database::Init();
  leveldown::Iterator::Init();
  leveldown::Batch::Init();

  node::js::propertyname nameDestroy = node::js::PropertyName(env, "destroy");
  node::js::propertyname nameRepair = node::js::PropertyName(env, "repair");
  node::js::propertyname nameLeveldown = node::js::PropertyName(env, "leveldown");

  node::js::value leveldown = node::js::CreateFunction(env, LevelDOWN);

  node::js::SetProperty(env, leveldown, nameDestroy, node::js::CreateFunction(env, DestroyDB));
  node::js::SetProperty(env, leveldown, nameRepair, node::js::CreateFunction(env, RepairDB));

  node::js::SetProperty(env, target, nameLeveldown, leveldown);
}

void Init (v8::Local<v8::Object> target, v8::Local<v8::Object> module) {
  node::js::legacy::WorkaroundNewModuleInit(target, module, NewInit);
}

NODE_MODULE(leveldown, Init)

} // namespace leveldown
