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

// NOTE (ianhall): This macro is never used, but it is converted here for completeness
#define LD_RETURN_CALLBACK_OR_ERROR(callback, msg)                             \
  if (callback != nullptr                                                      \
      && napi_function == napi_get_type_of_value(env, callback)) {             \
    napi_value argv[] = {                                                      \
      napi_create_error(env, napi_create_string(env, msg))                     \
    };                                                                         \
    LD_RUN_CALLBACK(callback, 1, argv)                                         \
    napi_set_return_value(env, info, napi_get_undefined(env));                 \
    return;                                                                    \
  }                                                                            \
  return napi_throw_error(                                                     \
      env, napi_create_error(env, napi_create_string(env, msg)));

#define LD_RUN_CALLBACK(callback, argc, argv)                                  \
  napi_make_callback(                                                          \
      env, napi_get_global_scope(env), callback, argc, argv);

/* LD_METHOD_SETUP_COMMON setup the following objects:
 *  - Database* database
 *  - napi_value optionsObj (may be empty)
 *  - napi_value callback (won't be empty)
 * Will throw/return if there isn't a callback in arg 0 or 1
 */
// TODO (ianhall): We need a convenience api for throwing an error of any type with a message coming directly from a C string (copy Nan API)
#define MAX(a, b) ((a) > (b) ? (a) : (b))
#define LD_METHOD_SETUP_COMMON(name, optionPos, callbackPos)                   \
  int argsLength = napi_get_cb_args_length(env, info);                         \
  if (argsLength == 0) {                                                       \
    return napi_throw_error(env,                                               \
      napi_create_error(env,                                                   \
        napi_create_string(env, #name "() requires a callback argument")));    \
  }                                                                            \
  napi_value args[MAX(optionPos+1, callbackPos+1)];                            \
  napi_get_cb_args(env, info, args, MAX(optionPos+1, callbackPos+1));          \
  napi_value _this = napi_get_cb_this(env, info);                              \
  leveldown::Database* database =                                              \
    static_cast<leveldown::Database*>(napi_unwrap(env, _this));                \
  napi_value optionsObj = nullptr;                                             \
  napi_value callback = nullptr;                                               \
  if (optionPos == -1 &&                                                       \
      napi_get_type_of_value(env, args[callbackPos]) == napi_function) {       \
    callback = args[callbackPos];                                              \
  } else if (optionPos != -1 &&                                                \
      napi_get_type_of_value(env, args[callbackPos - 1]) == napi_function) {   \
    callback = args[callbackPos - 1];                                          \
  } else if (optionPos != -1                                                   \
        && napi_get_type_of_value(env, args[optionPos]) == napi_object         \
        && napi_get_type_of_value(env, args[callbackPos]) == napi_function) {  \
    optionsObj = args[optionPos];                                              \
    callback = args[callbackPos];                                              \
  } else {                                                                     \
    return napi_throw_error(env,                                               \
      napi_create_error(env,                                                   \
        napi_create_string(env, #name "() requires a callback argument")));    \
  }

#define LD_METHOD_SETUP_COMMON_ONEARG(name) LD_METHOD_SETUP_COMMON(name, -1, 0)

#endif
