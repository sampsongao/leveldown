/* Copyright (c) 2012-2016 LevelDOWN contributors
 * See list at <https://github.com/level/leveldown#contributing>
 * MIT License <https://github.com/level/leveldown/blob/master/LICENSE.md>
 */
#ifndef LD_LEVELDOWN_H
#define LD_LEVELDOWN_H

#include <napi.h>
#include <leveldb/slice.h>

#define NAPI_METHOD(name) \
  napi_value name(napi_env env, napi_callback_info info)

namespace Napi {
  typedef FunctionReference Callback;
}

#define CHECK_NAPI_RESULT(condition) (assert((condition) == napi_ok))

static inline size_t StringOrBufferLength(napi_env env, napi_value obj) {
  Napi::HandleScope scope(env);
  bool result;
  CHECK_NAPI_RESULT(napi_is_buffer(env, obj, &result));

  size_t sz;
  if (result) {
    CHECK_NAPI_RESULT(napi_get_buffer_info(env, obj, nullptr, &sz));
  }
  else {
    CHECK_NAPI_RESULT(napi_get_value_string_utf8(env, obj, nullptr, 0, &sz));
  }

  return sz;
}

static inline void DisposeStringOrBufferFromSlice(
        napi_env env
      , napi_value handle
      , leveldb::Slice slice) {

  bool result;
  CHECK_NAPI_RESULT(napi_is_buffer(env, handle, &result));

  if (!slice.empty() && !result)
    delete[] slice.data();
}

// NOTE: must call DisposeStringOrBufferFromSlice() on objects created here
// TODO (ianhall): The use of napi_get_string_utf8 below changes behavior of
// the original leveldown code by adding the v8::String::REPLACE_INVALID_UTF8
// flag to the WriteUtf8 call.  The napi needs to abstract or deal with
// v8::String flags in some way compatible with existing native modules.
#define LD_STRING_OR_BUFFER_TO_SLICE(to, from, name)                                        \
  size_t to ## Sz_;                                                                         \
  char* to ## Ch_;                                                                          \
  {                                                                                         \
    napi_valuetype from ## Type_;                                                           \
    CHECK_NAPI_RESULT(napi_typeof(env, from, &(from ## Type_)));                 \
    if (from ## Type_ == napi_null || from ## Type_ == napi_undefined) {                    \
      to ## Sz_ = 0;                                                                        \
      to ## Ch_ = 0;                                                                        \
    } else {                                                                                \
      napi_value from ## Object_ ;                                                          \
      CHECK_NAPI_RESULT(napi_coerce_to_object(env, from, &(from ## Object_)));              \
      bool result = false;                                                                  \
      if (from ## Object_ != nullptr) {                                                     \
        CHECK_NAPI_RESULT(napi_is_buffer(env, from ## Object_, &result));                   \
      }                                                                                     \
      if (result) {                                                                         \
        CHECK_NAPI_RESULT(napi_get_buffer_info(                                             \
          env, from ## Object_, (void**)&(to ##Ch_), &(to ## Sz_)));                        \
      } else {                                                                              \
        napi_value to ## Str_;                                                              \
        CHECK_NAPI_RESULT(napi_coerce_to_string(env, from, &(to ## Str_)));                 \
        size_t sz;                                                                          \
        CHECK_NAPI_RESULT(napi_get_value_string_utf8(env, to ## Str_, nullptr, 0, &sz));    \
        to ## Sz_ = sz;                                                                     \
        to ## Ch_ = new char[to ## Sz_+1];                                                  \
        size_t unused;                                                                      \
        CHECK_NAPI_RESULT(                                                                  \
          napi_get_value_string_utf8(env, to ## Str_, to ## Ch_, to ## Sz_+1, &unused));    \
      }                                                                                     \
    }                                                                                       \
  }                                                                                         \
  leveldb::Slice to(to ## Ch_, to ## Sz_);

#define LD_STRING_OR_BUFFER_TO_COPY(to, from, name)                            \
  size_t to ## Sz_;                                                            \
  char* to ## Ch_;                                                             \
  {                                                                            \
    napi_value from ## Object_;                                                \
    CHECK_NAPI_RESULT(napi_coerce_to_object(env, from, &(from ## Object_)));   \
    bool r = false;                                                            \
    if (from ## Object_ != nullptr) {                                          \
      CHECK_NAPI_RESULT(napi_is_buffer(env, from ## Object_, &r));             \
    }                                                                          \
    if (r) {                                                                   \
      CHECK_NAPI_RESULT(napi_get_buffer_info(                                  \
        env, from ## Object_, nullptr, &(to ## Sz_)));                         \
      to ## Ch_ = new char[to ## Sz_];                                         \
      char* buf = nullptr;                                                     \
      CHECK_NAPI_RESULT(napi_get_buffer_info(                                  \
        env, from ## Object_, (void**)&buf, nullptr));                         \
      memcpy(to ## Ch_, buf, to ## Sz_);                                       \
    } else {                                                                   \
      napi_value to ## Str_;                                                   \
      CHECK_NAPI_RESULT(napi_coerce_to_string(env, from, &(to ## Str_)));      \
      size_t sz;                                                               \
      CHECK_NAPI_RESULT(napi_get_value_string_utf8(                            \
        env, to ## Str_, nullptr, 0, &sz));                                    \
      to ## Sz_ = sz;                                                          \
      to ## Ch_ = new char[to ## Sz_+1];                                       \
      size_t unused;                                                           \
      CHECK_NAPI_RESULT(napi_get_value_string_utf8(                            \
        env, to ## Str_, to ## Ch_, to ## Sz_+1, &unused));                    \
    }                                                                          \
  }

// NOTE (ianhall): This macro is never used, but it is converted here for completeness
#define LD_RETURN_CALLBACK_OR_ERROR(callback, msg)                             \
  if (callback != nullptr) {                                                   \
    napi_valuetype t;                                                          \
    CHECK_NAPI_RESULT(napi_typeof(env, callback, &t));              \
    if (t == napi_function) {                                                  \
      napi_value str;                                                          \
      napi_value err;                                                          \
      CHECK_NAPI_RESULT(napi_create_string(env, msg, &str));                   \
      CHECK_NAPI_RESULT(napi_create_error(env, nullptr, str, err));                     \
      napi_value argv[] = {                                                    \
        napi_create_error(env, nullptr, napi_create_string(env, msg))                   \
      };                                                                       \
      LD_RUN_CALLBACK(callback, 1, argv)                                       \
      napi_value undefined;                                                    \
      CHECK_NAPI_RESULT(napi_get_undefined(env, &undefined));                  \
      CHECK_NAPI_RESULT(napi_set_return_value(env, info, undefined));          \
      return;                                                                  \
    }                                                                          \
  }                                                                            \
  CHECK_NAPI_RESULT(napi_throw_error(env, nullptr, msg));                               \
  return;

#define LD_RUN_CALLBACK(callback, argc, argv)                                  \
  do {                                                                         \
    napi_value g;                                                              \
    CHECK_NAPI_RESULT(napi_get_global(env, &g));                               \
    napi_value unused;                                                         \
    CHECK_NAPI_RESULT(                                                         \
      napi_make_callback(env, g, callback, argc, argv, &unused));              \
  } while(0)

/* LD_METHOD_SETUP_COMMON setup the following objects:
 *  - Database* database
 *  - napi_value optionsObj (may be empty)
 *  - napi_value callback (won't be empty)
 * Will throw/return if there isn't a callback in arg 0 or 1
 */
#define MAX(a, b) ((a) > (b) ? (a) : (b))
#define LD_METHOD_SETUP_COMMON(name, optionPos, callbackPos)                   \
  size_t argsLength = MAX(optionPos+1, callbackPos+1);                         \
  napi_value args[MAX(optionPos+1, callbackPos+1)];                            \
  napi_value _this;                                                            \
  CHECK_NAPI_RESULT(                                                           \
    napi_get_cb_info(env, info, &argsLength, args, &_this, nullptr));          \
  if (argsLength == 0) {                                                       \
    CHECK_NAPI_RESULT(                                                         \
      napi_throw_error(env, nullptr, #name "() requires a callback argument"));         \
    return nullptr;                                                            \
  }                                                                            \
  void* unwrapped;                                                             \
  CHECK_NAPI_RESULT(napi_unwrap(env, _this, &unwrapped));                      \
  leveldown::Database* database =                                              \
    static_cast<leveldown::Database*>(unwrapped);                              \
  napi_value optionsObj = nullptr;                                             \
  napi_value callback = nullptr;                                               \
  if (optionPos == -1) {                                                       \
    napi_valuetype t;                                                          \
    CHECK_NAPI_RESULT(napi_typeof(env, args[callbackPos], &t));                \
    if (t == napi_function) {                                                  \
      callback = args[callbackPos];                                            \
    }                                                                          \
  } else {                                                                     \
    napi_valuetype t;                                                          \
    CHECK_NAPI_RESULT(napi_typeof(env, args[callbackPos - 1], &t));            \
    if (t == napi_function) {                                                  \
      callback = args[callbackPos - 1];                                        \
    } else {                                                                   \
      CHECK_NAPI_RESULT(napi_typeof(env, args[optionPos], &t));                \
      if (t == napi_object) {                                                  \
        CHECK_NAPI_RESULT(napi_typeof(env, args[callbackPos], &t)); \
        if (t == napi_function) {                                              \
          optionsObj = args[optionPos];                                        \
          callback = args[callbackPos];                                        \
        }                                                                      \
      }                                                                        \
    }                                                                          \
  }                                                                            \
  if (!callback) {                                                             \
    CHECK_NAPI_RESULT(                                                         \
      napi_throw_error(env, nullptr, #name "() requires a callback argument"));         \
  }

#define LD_METHOD_SETUP_COMMON_ONEARG(name) LD_METHOD_SETUP_COMMON(name, -1, 0)

#endif
