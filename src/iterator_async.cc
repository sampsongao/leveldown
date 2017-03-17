/* Copyright (c) 2012-2016 LevelDOWN contributors
 * See list at <https://github.com/level/leveldown#contributing>
 * MIT License <https://github.com/level/leveldown/blob/master/LICENSE.md>
 */

#include <napi.h>

#include "database.h"
#include "leveldown.h"
#include "async.h"
#include "iterator_async.h"

namespace leveldown {

/** NEXT-MULTI WORKER **/

NextWorker::NextWorker (
    Iterator* iterator
  , napi_env env
  , napi_value callback
  , void (*localCallback)(Iterator*)
) : AsyncWorker(NULL, env, callback)
  , iterator(iterator)
  , localCallback(localCallback)
{};

NextWorker::~NextWorker () {}

void NextWorker::Execute () {
  ok = iterator->IteratorNext(result);
  if (!ok)
    SetStatus(iterator->IteratorStatus());
}

void NextWorker::OnOK () {
  napi_env env = Env();
  Napi::HandleScope scope(env);
  size_t idx = 0;

  size_t arraySize = result.size() * 2;
  napi_value returnArray;
  CHECK_NAPI_RESULT(napi_create_array_with_length(env, arraySize, &returnArray));

  for(idx = 0; idx < result.size(); ++idx) {
    std::pair<std::string, std::string> row = result[idx];
    std::string key = row.first;
    std::string value = row.second;

    napi_value returnKey;
    if (iterator->keyAsBuffer) {
      //TODO: use NewBuffer, see database_async.cc
      CHECK_NAPI_RESULT(napi_create_buffer_copy(env, key.data(), key.size(), &returnKey));
    } else {
      CHECK_NAPI_RESULT(napi_create_string_utf8(env, (char*)key.data(), key.size(), &returnKey));
    }

    napi_value returnValue;
    if (iterator->valueAsBuffer) {
      //TODO: use NewBuffer, see database_async.cc
      CHECK_NAPI_RESULT(napi_create_buffer_copy(env, value.data(), value.size(), &returnValue));
    } else {
      CHECK_NAPI_RESULT(napi_create_string_utf8(env, (char*)value.data(), value.size(), &returnValue));
    }

    // put the key & value in a descending order, so that they can be .pop:ed in javascript-land
    CHECK_NAPI_RESULT(napi_set_element(env, returnArray, arraySize - idx * 2 - 1, returnKey));
    CHECK_NAPI_RESULT(napi_set_element(env, returnArray, arraySize - idx * 2 - 2, returnValue));
  }

  // clean up & handle the next/end state see iterator.cc/checkEndCallback
  localCallback(iterator);

  napi_value nullVal;
  CHECK_NAPI_RESULT(napi_get_null(env, &nullVal));
  napi_value boolVal;
  CHECK_NAPI_RESULT(napi_create_boolean(env, !ok, &boolVal));

  napi_value globalVal;
  CHECK_NAPI_RESULT(napi_get_global(Env(), &globalVal));

  _callback.Call(globalVal, {
    nullVal,
    returnArray,
    // when ok === false all data has been read, so it's then finished
    boolVal,
  });
}

/** END WORKER **/

EndWorker::EndWorker (
    Iterator* iterator
  , napi_env env
  , napi_value callback
) : AsyncWorker(NULL, env, callback)
  , iterator(iterator)
{};

EndWorker::~EndWorker () { }

void EndWorker::Execute () {
  iterator->IteratorEnd();
}

void EndWorker::OnOK () {
  iterator->Release();

  napi_value globalVal;
  CHECK_NAPI_RESULT(napi_get_global(Env(), &globalVal));

  _callback.Call(globalVal, {});
}

} // namespace leveldown
