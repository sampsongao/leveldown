/* Copyright (c) 2012-2016 LevelDOWN contributors
 * See list at <https://github.com/level/leveldown#contributing>
 * MIT License <https://github.com/level/leveldown/blob/master/LICENSE.md>
 */

#ifndef LD_COMMON_H
#define LD_COMMON_H


namespace leveldown {

inline bool BooleanOptionValue(napi_env env,
                                   napi_value options,
                                   const char* _key,
                                   bool def = false) {
  Napi::HandleScope scope;
  napi_propertyname key = napi_property_name(env, _key);
  return options != nullptr
    && napi_has_property(env, options, key)
    ? napi_get_value_bool(env, napi_get_property(env, options, key))
    : def;
}

inline uint32_t UInt32OptionValue(napi_env env,
                                      napi_value options,
                                      const char* _key,
                                      uint32_t def) {
  Napi::HandleScope scope;
  napi_propertyname key = napi_property_name(env, _key);
  napi_value value;
  return options != nullptr
    && napi_has_property(env, options, key)
    && napi_number == napi_get_type_of_value(env, value = napi_get_property(env, options, key))
    ? napi_get_value_uint32(env, value)
    : def;
}

} // namespace leveldown

#endif
