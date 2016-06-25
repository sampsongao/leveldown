/* Copyright (c) 2012-2016 LevelDOWN contributors
 * See list at <https://github.com/level/leveldown#contributing>
 * MIT License <https://github.com/level/leveldown/blob/master/LICENSE.md>
 */
#ifndef LD_LEVELDOWN_H
#define LD_LEVELDOWN_H

#include <node.h>
#include <node_buffer.h>
#include <leveldb/slice.h>
#include <nan.h>

static inline size_t StringOrBufferLength(v8::Local<v8::Value> obj) {
  Nan::HandleScope scope;

  return (!obj->ToObject().IsEmpty()
    && node::Buffer::HasInstance(obj->ToObject()))
    ? node::Buffer::Length(obj->ToObject())
    : obj->ToString()->Utf8Length();
}

// NOTE: this MUST be called on objects created by
// LD_STRING_OR_BUFFER_TO_SLICE
static inline void DisposeStringOrBufferFromSlice(
        Nan::Persistent<v8::Object> &handle
      , leveldb::Slice slice) {
  Nan::HandleScope scope;

  if (!slice.empty()) {
    v8::Local<v8::Value> obj = Nan::New<v8::Object>(handle)->Get(Nan::New<v8::String>("obj").ToLocalChecked());
    if (!node::Buffer::HasInstance(obj))
      delete[] slice.data();
  }

  handle.Reset();
}

static inline void DisposeStringOrBufferFromSlice(
        v8::Local<v8::Value> handle
      , leveldb::Slice slice) {

  if (!slice.empty() && !node::Buffer::HasInstance(handle))
    delete[] slice.data();
}

// NOTE: must call DisposeStringOrBufferFromSlice() on objects created here
#define LD_STRING_OR_BUFFER_TO_SLICE(to, from, name)                           \
  size_t to ## Sz_;                                                            \
  char* to ## Ch_;                                                             \
  if (from->IsNull() || from->IsUndefined()) {                                 \
    to ## Sz_ = 0;                                                             \
    to ## Ch_ = 0;                                                             \
  } else if (!from->ToObject().IsEmpty()                                       \
      && node::Buffer::HasInstance(from->ToObject())) {                        \
    to ## Sz_ = node::Buffer::Length(from->ToObject());                        \
    to ## Ch_ = node::Buffer::Data(from->ToObject());                          \
  } else {                                                                     \
    v8::Local<v8::String> to ## Str = from->ToString();                        \
    to ## Sz_ = to ## Str->Utf8Length();                                       \
    to ## Ch_ = new char[to ## Sz_];                                           \
    to ## Str->WriteUtf8(                                                      \
        to ## Ch_                                                              \
      , -1                                                                     \
      , NULL, v8::String::NO_NULL_TERMINATION                                  \
    );                                                                         \
  }                                                                            \
  leveldb::Slice to(to ## Ch_, to ## Sz_);

#define LD_STRING_OR_BUFFER_TO_COPY(to, from, name)                            \
  size_t to ## Sz_;                                                            \
  char* to ## Ch_;                                                             \
  if (!from->ToObject().IsEmpty()                                              \
      && node::Buffer::HasInstance(from->ToObject())) {                        \
    to ## Sz_ = node::Buffer::Length(from->ToObject());                        \
    to ## Ch_ = new char[to ## Sz_];                                           \
    memcpy(to ## Ch_, node::Buffer::Data(from->ToObject()), to ## Sz_);        \
  } else {                                                                     \
    v8::Local<v8::String> to ## Str = from->ToString();                        \
    to ## Sz_ = to ## Str->Utf8Length();                                       \
    to ## Ch_ = new char[to ## Sz_];                                           \
    to ## Str->WriteUtf8(                                                      \
        to ## Ch_                                                              \
      , -1                                                                     \
      , NULL, v8::String::NO_NULL_TERMINATION                                  \
    );                                                                         \
  }

#define LD_RETURN_CALLBACK_OR_ERROR(callback, msg)                             \
  if (!callback.IsEmpty() && callback->IsFunction()) {                         \
    v8::Local<v8::Value> argv[] = {                                            \
      Nan::Error(msg)                                                          \
    };                                                                         \
    LD_RUN_CALLBACK(callback, 1, argv)                                         \
    info.GetReturnValue().SetUndefined();                                      \
    return;                                                                    \
  }                                                                            \
  return Nan::ThrowError(msg);

#define LD_RUN_CALLBACK(callback, argc, argv)                                  \
  Nan::MakeCallback(                                                           \
      Nan::GetCurrentContext()->Global(), callback, argc, argv);

/* LD_METHOD_SETUP_COMMON setup the following objects:
 *  - Database* database
 *  - v8::Local<v8::Object> optionsObj (may be empty)
 *  - Nan::Persistent<v8::Function> callback (won't be empty)
 * Will throw/return if there isn't a callback in arg 0 or 1
 */
#define LD_METHOD_SETUP_COMMON(name, optionPos, callbackPos)                   \
  if (info.Length() == 0)                                                      \
    return Nan::ThrowError(#name "() requires a callback argument");           \
  leveldown::Database* database =                                              \
    Nan::ObjectWrap::Unwrap<leveldown::Database>(info.This());                 \
  v8::Local<v8::Object> optionsObj;                                            \
  v8::Local<v8::Function> callback;                                            \
  if (optionPos == -1 && info[callbackPos]->IsFunction()) {                    \
    callback = info[callbackPos].As<v8::Function>();                           \
  } else if (optionPos != -1 && info[callbackPos - 1]->IsFunction()) {         \
    callback = info[callbackPos - 1].As<v8::Function>();                       \
  } else if (optionPos != -1                                                   \
        && info[optionPos]->IsObject()                                         \
        && info[callbackPos]->IsFunction()) {                                  \
    optionsObj = info[optionPos].As<v8::Object>();                             \
    callback = info[callbackPos].As<v8::Function>();                           \
  } else {                                                                     \
    return Nan::ThrowError(#name "() requires a callback argument");           \
  }

#define MAX(a, b) ((a) > (b) ? (a) : (b))

// TODO (ianhall): We need IsObject() and IsFunction() APIs to complete this conversion
// TODO (ianhall): We need a convenience for throwing an error
// TODO (ianhall): We need a CreateError() API to use instead of CreateTypeError()
// NAPI NOTE (ianhall): API should have convenience for throwing new errors of different types with given string message (copy Nan API)
#define LD_METHOD_SETUP_COMMON_NAPI(name, optionPos, callbackPos)              \
  int argsLength = node::js::GetCallbackArgsLength(env, info);                 \
  if (argsLength == 0) {                                                       \
    return node::js::ThrowError(env,                                           \
      node::js::CreateTypeError(env,                                           \
        node::js::CreateString(env, #name "() requires a callback argument")));\
  }                                                                            \
  node::js::value args[MAX(optionPos+1, callbackPos+1)];                       \
  node::js::GetCallbackArgs(env, info, args, MAX(optionPos+1, callbackPos+1)); \
  node::js::value thisObj = node::js::GetCallbackObject(env, info);            \
  leveldown::Database* database =                                              \
    static_cast<leveldown::Database*>(node::js::Unwrap(env, thisObj));         \
  node::js::value optionsObjNapi = nullptr;                                    \
  node::js::value callbackNapi = nullptr;                                      \
  if (optionPos == -1 &&                                                       \
      node::js::legacy::V8LocalValue(args[callbackPos])->IsFunction()) {       \
    callbackNapi = args[callbackPos];                                          \
  } else if (optionPos != -1 &&                                                \
      node::js::legacy::V8LocalValue(args[callbackPos - 1])->IsFunction()) {   \
    callbackNapi = args[callbackPos - 1];                                      \
  } else if (optionPos != -1                                                   \
        && node::js::legacy::V8LocalValue(args[optionPos])->IsObject()         \
        && node::js::legacy::V8LocalValue(args[callbackPos])->IsFunction()) {  \
    optionsObjNapi = args[optionPos];                                          \
    callbackNapi = args[callbackPos];                                          \
  } else {                                                                     \
    return node::js::ThrowError(env,                                           \
      node::js::CreateTypeError(env,                                           \
        node::js::CreateString(env, #name "() requires a callback argument")));\
  }

// TODO (ianhall): This is temporary, remove when no longer used (and rename objectObjNapi, callbackNapi above back to objectObj, callback)
#define LD_METHOD_SETUP_COMMON_NAPI_BACK_TO_V8                                 \
  v8::Local<v8::Object> info_This__ =                                          \
    node::js::legacy::V8LocalValue(thisObj).As<v8::Object>();                  \
  v8::Local<v8::Object> optionsObj;                                            \
  v8::Local<v8::Function> callback;                                            \
  if (optionsObjNapi != nullptr) {                                             \
    optionsObj = node::js::legacy::V8LocalValue(optionsObjNapi).As<v8::Object>();\
  }                                                                            \
  if (callbackNapi != nullptr) {                                               \
    callback = node::js::legacy::V8LocalValue(callbackNapi).As<v8::Function>();\
  }

#define LD_METHOD_SETUP_COMMON_ONEARG(name) LD_METHOD_SETUP_COMMON(name, -1, 0)
#define LD_METHOD_SETUP_COMMON_ONEARG_NAPI(name) LD_METHOD_SETUP_COMMON_NAPI(name, -1, 0)

// TODO (ianhall): This should be moved to node_jsvmapi.h (or elsewhere?)
#define NAPI_METHOD(name)                                                       \
    void name(node::js::env env, node::js::FunctionCallbackInfo info)

#endif
