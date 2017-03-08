/* Copyright (c) 2012-2016 LevelDOWN contributors
 * See list at <https://github.com/level/leveldown#contributing>
 * MIT License <https://github.com/level/leveldown/blob/master/LICENSE.md>
 */

#ifndef LD_COMMON_H
#define LD_COMMON_H

#define CHECK_NAPI_RESULT(condition) (assert((condition) == napi_ok))

namespace leveldown {

inline bool BooleanOptionValue(napi_env env,
                                   napi_value options,
                                   const char* _key,
                                   bool def = false) {
  Napi::HandleScope scope(env);
  napi_propertyname key;
  CHECK_NAPI_RESULT(napi_property_name(env, _key, &key));

  if (options) {
    bool result;
    CHECK_NAPI_RESULT(napi_has_property(env, options, key, &result));

    if (result) {
      napi_value v;
      CHECK_NAPI_RESULT(napi_get_property(env, options, key, &v));
      CHECK_NAPI_RESULT(napi_get_value_bool(env, v, &result));
      return result;
    }
  }

  return def;
}

inline uint32_t UInt32OptionValue(napi_env env,
                                      napi_value options,
                                      const char* _key,
                                      uint32_t def) {
  Napi::HandleScope scope(env);
  napi_propertyname key;
  CHECK_NAPI_RESULT(napi_property_name(env, _key, &key));

  if (options) {
    bool result;
    CHECK_NAPI_RESULT(napi_has_property(env, options, key, &result));

    if (result) {
      napi_value v;
      CHECK_NAPI_RESULT(napi_get_property(env, options, key, &v));
      napi_valuetype t;
      CHECK_NAPI_RESULT(napi_get_type_of_value(env, v, &t));

      if (t == napi_number) {
        uint32_t u;
        CHECK_NAPI_RESULT(napi_get_value_uint32(env, v, &u));
        return u;
      }
    }
  }

  return def;
}

} // namespace leveldown

#endif
