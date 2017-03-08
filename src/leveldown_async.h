/* Copyright (c) 2012-2016 LevelDOWN contributors
 * See list at <https://github.com/level/leveldown#contributing>
 * MIT License <https://github.com/level/leveldown/blob/master/LICENSE.md>
 */

#ifndef LD_LEVELDOWN_ASYNC_H
#define LD_LEVELDOWN_ASYNC_H

#include <napi.h>

#include "async.h"

namespace leveldown {

class DestroyWorker : public AsyncWorker {
public:
  DestroyWorker (
      Napi::Utf8String* location
    , napi_value callback
  );

  virtual ~DestroyWorker ();
  virtual void Execute ();

private:
  Napi::Utf8String* location;
};

class RepairWorker : public AsyncWorker {
public:
  RepairWorker (
      Napi::Utf8String* location
    , napi_value callback
  );

  virtual ~RepairWorker ();
  virtual void Execute ();

private:
  Napi::Utf8String* location;
};

} // namespace leveldown

#endif
