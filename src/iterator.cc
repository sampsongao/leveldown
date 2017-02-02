/* Copyright (c) 2012-2016 LevelDOWN contributors
 * See list at <https://github.com/level/leveldown#contributing>
 * MIT License <https://github.com/level/leveldown/blob/master/LICENSE.md>
 */

#include <node.h>
#include <node_buffer.h>

#include "database.h"
#include "iterator.h"
#include "iterator_async.h"
#include "common.h"

namespace leveldown {

static napi_persistent iterator_constructor;

Iterator::Iterator (
    Database* database
  , uint32_t id
  , leveldb::Slice* start
  , std::string* end
  , bool reverse
  , bool keys
  , bool values
  , int limit
  , std::string* lt
  , std::string* lte
  , std::string* gt
  , std::string* gte
  , bool fillCache
  , bool keyAsBuffer
  , bool valueAsBuffer
  , size_t highWaterMark
) : database(database)
  , id(id)
  , start(start)
  , end(end)
  , reverse(reverse)
  , keys(keys)
  , values(values)
  , limit(limit)
  , lt(lt)
  , lte(lte)
  , gt(gt)
  , gte(gte)
  , highWaterMark(highWaterMark)
  , keyAsBuffer(keyAsBuffer)
  , valueAsBuffer(valueAsBuffer)
{
  Napi::HandleScope scope;

  options    = new leveldb::ReadOptions();
  options->fill_cache = fillCache;
  // get a snapshot of the current state
  options->snapshot = database->NewSnapshot();
  dbIterator = NULL;
  count      = 0;
  seeking    = false;
  nexting    = false;
  ended      = false;
  endWorker  = NULL;
};

Iterator::~Iterator () {
  delete options;
  if (start != NULL) {
    // Special case for `start` option: it won't be
    // freed up by any of the delete calls below.
    if (!((lt != NULL && reverse)
        || (lte != NULL && reverse)
        || (gt != NULL && !reverse)
        || (gte != NULL && !reverse))) {
      delete[] start->data();
    }
    delete start;
  }
  if (end != NULL)
    delete end;
  if (lt != NULL)
    delete lt;
  if (gt != NULL)
    delete gt;
  if (lte != NULL)
    delete lte;
  if (gte != NULL)
    delete gte;
  
  napi_env env;
  CHECK_NAPI_RESULT(napi_get_current_env(&env));
  CHECK_NAPI_RESULT(napi_release_weakref(env, handle));
};

bool Iterator::GetIterator () {
  if (dbIterator == NULL) {
    dbIterator = database->NewIterator(options);

    if (start != NULL) {
      dbIterator->Seek(*start);

      if (reverse) {
        if (!dbIterator->Valid()) {
          // if it's past the last key, step back
          dbIterator->SeekToLast();
        } else {
          std::string key_ = dbIterator->key().ToString();

          if (lt != NULL) {
            if (lt->compare(key_) <= 0)
              dbIterator->Prev();
          } else if (lte != NULL) {
            if (lte->compare(key_) < 0)
              dbIterator->Prev();
          } else if (start != NULL) {
            if (start->compare(key_))
              dbIterator->Prev();
          }
        }

        if (dbIterator->Valid() && lt != NULL) {
          if (lt->compare(dbIterator->key().ToString()) <= 0)
            dbIterator->Prev();
        }
      } else {
        if (dbIterator->Valid() && gt != NULL
            && gt->compare(dbIterator->key().ToString()) == 0)
          dbIterator->Next();
      }
    } else if (reverse) {
      dbIterator->SeekToLast();
    } else {
      dbIterator->SeekToFirst();
    }

    return true;
  }
  return false;
}

bool Iterator::Read (std::string& key, std::string& value) {
  // if it's not the first call, move to next item.
  if (!GetIterator() && !seeking) {
    if (reverse)
      dbIterator->Prev();
    else
      dbIterator->Next();
  }

  seeking = false;

  // now check if this is the end or not, if not then return the key & value
  if (dbIterator->Valid()) {
    std::string key_ = dbIterator->key().ToString();
    int isEnd = end == NULL ? 1 : end->compare(key_);

    if ((limit < 0 || ++count <= limit)
      && (end == NULL
          || (reverse && (isEnd <= 0))
          || (!reverse && (isEnd >= 0)))
      && ( lt  != NULL ? (lt->compare(key_) > 0)
         : lte != NULL ? (lte->compare(key_) >= 0)
         : true )
      && ( gt  != NULL ? (gt->compare(key_) < 0)
         : gte != NULL ? (gte->compare(key_) <= 0)
         : true )
    ) {
      if (keys)
        key.assign(dbIterator->key().data(), dbIterator->key().size());
      if (values)
        value.assign(dbIterator->value().data(), dbIterator->value().size());
      return true;
    }
  }

  return false;
}

bool Iterator::IteratorNext (std::vector<std::pair<std::string, std::string> >& result) {
  size_t size = 0;
  while(true) {
    std::string key, value;
    bool ok = Read(key, value);

    if (ok) {
      result.push_back(std::make_pair(key, value));
      size = size + key.size() + value.size();

      if (size > highWaterMark)
        return true;

    } else {
      return false;
    }
  }
}

leveldb::Status Iterator::IteratorStatus () {
  return dbIterator->status();
}

void Iterator::IteratorEnd () {
  //TODO: could return it->status()
  delete dbIterator;
  dbIterator = NULL;
  database->ReleaseSnapshot(options->snapshot);
}

void Iterator::Release () {
  database->ReleaseIterator(id);
}

void checkEndCallback (Iterator* iterator) {
  iterator->nexting = false;
  if (iterator->endWorker != NULL) {
    Napi::AsyncQueueWorker(iterator->endWorker);
    iterator->endWorker = NULL;
  }
}

NAPI_METHOD(Iterator::Seek) {
  napi_value args[1];
  CHECK_NAPI_RESULT(napi_get_cb_args(env, info, args, 1));
  napi_value _this;
  CHECK_NAPI_RESULT(napi_get_cb_this(env, info, &_this));
  void* unwrapped;
  CHECK_NAPI_RESULT(napi_unwrap(env, _this, &unwrapped));
  Iterator* iterator = static_cast<Iterator*>(unwrapped);
  iterator->GetIterator();
  leveldb::Iterator* dbIterator = iterator->dbIterator;
  Napi::Utf8String key(args[0]);

  dbIterator->Seek(*key);
  iterator->seeking = true;

  if (dbIterator->Valid()) {
    int cmp = dbIterator->key().compare(*key);
    if (cmp > 0 && iterator->reverse) {
      dbIterator->Prev();
    } else if (cmp < 0 && !iterator->reverse) {
      dbIterator->Next();
    }
  } else {
    if (iterator->reverse) {
      dbIterator->SeekToLast();
    } else {
      dbIterator->SeekToFirst();
    }
    if (dbIterator->Valid()) {
      int cmp = dbIterator->key().compare(*key);
      if (cmp > 0 && iterator->reverse) {
        dbIterator->SeekToFirst();
        dbIterator->Prev();
      } else if (cmp < 0 && !iterator->reverse) {
        dbIterator->SeekToLast();
        dbIterator->Next();
      }
    }
  }

  napi_value holder;
  CHECK_NAPI_RESULT(napi_get_cb_holder(env, info, &holder));
  CHECK_NAPI_RESULT(napi_set_return_value(env, info, holder));
}

NAPI_METHOD(Iterator::Next) {
  napi_value args[1];
  CHECK_NAPI_RESULT(napi_get_cb_args(env, info, args, 1));
  napi_value _this;
  CHECK_NAPI_RESULT(napi_get_cb_this(env, info, &_this));
  void* unwrapped;
  CHECK_NAPI_RESULT(napi_unwrap(env, _this, &unwrapped));
  Iterator* iterator = static_cast<Iterator*>(unwrapped);

  napi_valuetype t;
  CHECK_NAPI_RESULT(napi_get_type_of_value(env, args[0], &t));
  if (t != napi_function) {
    CHECK_NAPI_RESULT(napi_throw_error(env, "next() requires a callback argument"));
    return;
  }

  napi_value callback = args[0];

  NextWorker* worker = new NextWorker(
      iterator
    , callback
    , checkEndCallback
  );
  // persist to prevent accidental GC
  worker->SaveToPersistent("iterator", _this);
  iterator->nexting = true;
  Napi::AsyncQueueWorker(worker);

  napi_value holder;
  CHECK_NAPI_RESULT(napi_get_cb_holder(env, info, &holder));
  CHECK_NAPI_RESULT(napi_set_return_value(env, info, holder));
}

NAPI_METHOD(Iterator::End) {
  napi_value args[1];
  CHECK_NAPI_RESULT(napi_get_cb_args(env, info, args, 1));
  napi_value _this;
  CHECK_NAPI_RESULT(napi_get_cb_this(env, info, &_this));
  void* unwrapped;
  CHECK_NAPI_RESULT(napi_unwrap(env, _this, &unwrapped));
  Iterator* iterator = static_cast<Iterator*>(unwrapped);
  napi_valuetype t;
  CHECK_NAPI_RESULT(napi_get_type_of_value(env, args[0], &t));
  if (t != napi_function) {
      CHECK_NAPI_RESULT(napi_throw_error(env, "end() requires a callback argument"));
      return;
  }

  if (!iterator->ended) {
    napi_value callback = args[0];

    EndWorker* worker = new EndWorker(
        iterator
      , callback
    );
    // persist to prevent accidental GC
    worker->SaveToPersistent("iterator", _this);
    iterator->ended = true;

    if (iterator->nexting) {
      // waiting for a next() to return, queue the end
      iterator->endWorker = worker;
    } else {
      Napi::AsyncQueueWorker(worker);
    }
  }

  napi_value holder;
  CHECK_NAPI_RESULT(napi_get_cb_holder(env, info, &holder));
  CHECK_NAPI_RESULT(napi_set_return_value(env, info, holder));
}

void Iterator::Init (napi_env env) {
  napi_property_descriptor methods [] = {
    { "seek", Iterator::Seek },
    { "next", Iterator::Next },
    { "end", Iterator::End },
  };

  napi_value ctor;
  CHECK_NAPI_RESULT(napi_create_constructor(env, "Iterator", Iterator::New, nullptr, 3, methods, &ctor));
  CHECK_NAPI_RESULT(napi_create_persistent(env, ctor, &iterator_constructor));
}

napi_value Iterator::NewInstance (
        napi_env env
      , napi_value database
      , napi_value id
      , napi_value optionsObj
    ) {

  Napi::EscapableHandleScope scope(env);

  napi_value instance;
  napi_value constructorHandle;
  CHECK_NAPI_RESULT(napi_get_persistent_value(env, iterator_constructor, &constructorHandle));

  if (optionsObj == nullptr) {
    napi_value argv[2] = { database, id };
    CHECK_NAPI_RESULT(napi_new_instance(env, constructorHandle, 2, argv, &instance));
  } else {
    napi_value argv[3] = { database, id, optionsObj };
    CHECK_NAPI_RESULT(napi_new_instance(env, constructorHandle, 3, argv, &instance));
  }

  return scope.Escape(instance);
}

NAPI_METHOD(Iterator::New) {
  napi_value args[3];
  CHECK_NAPI_RESULT(napi_get_cb_args(env, info, args, 3));
  int argsLength;
  CHECK_NAPI_RESULT(napi_get_cb_args_length(env, info, &argsLength));
  void* unwrapped;
  CHECK_NAPI_RESULT(napi_unwrap(env, args[0], &unwrapped));
  Database* database = static_cast<Database*>(unwrapped);

  leveldb::Slice* start = NULL;
  std::string* end = NULL;
  int limit = -1;
  // default highWaterMark from Readble-streams
  size_t highWaterMark = 16 * 1024;

  napi_value id = args[1];

  napi_value optionsObj = nullptr;

  char *startStr = NULL;
  std::string* lt = NULL;
  std::string* lte = NULL;
  std::string* gt = NULL;
  std::string* gte = NULL;

  //default to forward.
  bool reverse = false;

  if (argsLength > 1) {
      napi_valuetype t;
      CHECK_NAPI_RESULT(napi_get_type_of_value(env, args[2], &t));

      if (t == napi_object) {
          optionsObj = args[2];
          reverse = BooleanOptionValue(env, optionsObj, "reverse");

          napi_propertyname pnStart;
          CHECK_NAPI_RESULT(napi_property_name(env, "start", &pnStart));
          napi_value valStart = nullptr;
          bool r;
          CHECK_NAPI_RESULT(napi_has_property(env, optionsObj, pnStart, &r));

          if (r) {
              CHECK_NAPI_RESULT(napi_get_property(env, optionsObj, pnStart, &valStart));
              CHECK_NAPI_RESULT(napi_buffer_has_instance(env, valStart, &r));
              CHECK_NAPI_RESULT(napi_get_type_of_value(env, valStart, &t));

              if (r || t == napi_string) {
                  napi_value startBuffer = valStart;

                  // ignore start if it has size 0 since a Slice can't have length 0
                  if (StringOrBufferLength(env, startBuffer) > 0) {
                      LD_STRING_OR_BUFFER_TO_COPY(_start, startBuffer, start)
                          start = new leveldb::Slice(_startCh_, _startSz_);
                      startStr = _startCh_;
                  }
              }
          }

          napi_propertyname pnEnd;
          CHECK_NAPI_RESULT(napi_property_name(env, "end", &pnEnd));
          napi_value valEnd = nullptr;
          CHECK_NAPI_RESULT(napi_has_property(env, optionsObj, pnEnd, &r));

          if (r) {
              CHECK_NAPI_RESULT(napi_get_property(env, optionsObj, pnEnd, &valEnd));
              CHECK_NAPI_RESULT(napi_buffer_has_instance(env, valEnd, &r));
              CHECK_NAPI_RESULT(napi_get_type_of_value(env, valEnd, &t));

              if (r || t == napi_string) {
                  napi_value endBuffer = valEnd;

                  // ignore end if it has size 0 since a Slice can't have length 0
                  if (StringOrBufferLength(env, endBuffer) > 0) {
                      LD_STRING_OR_BUFFER_TO_COPY(_end, endBuffer, end)
                          end = new std::string(_endCh_, _endSz_);
                      delete[] _endCh_;
                  }
              }
          }

          int64_t i64;
          napi_propertyname pnLimit;
          CHECK_NAPI_RESULT(napi_property_name(env, "limit", &pnLimit));
          CHECK_NAPI_RESULT(napi_has_property(env, optionsObj, pnLimit, &r));

          if (r) {
              napi_value valLimit;
              CHECK_NAPI_RESULT(napi_get_property(env, optionsObj, pnLimit, &valLimit));
              CHECK_NAPI_RESULT(napi_get_value_int64(env, valLimit, &i64));
              // TODO: is this truncated?
              limit = i64;
          }

          napi_propertyname pnHighWaterMark;
          CHECK_NAPI_RESULT(napi_property_name(env, "highWaterMark", &pnHighWaterMark));
          CHECK_NAPI_RESULT(napi_has_property(env, optionsObj, pnHighWaterMark, &r));
          if (r) {
              napi_value valHighWaterMark;
              CHECK_NAPI_RESULT(napi_get_property(env, optionsObj, pnHighWaterMark, &valHighWaterMark));
              CHECK_NAPI_RESULT(napi_get_value_int64(env, valHighWaterMark, &i64));
              // TODO: is this truncated?
              highWaterMark = i64;
          }

          napi_propertyname pnLt;
          CHECK_NAPI_RESULT(napi_property_name(env, "lt", &pnLt));
          CHECK_NAPI_RESULT(napi_has_property(env, optionsObj, pnLt, &r));
          if (r) {
              napi_value valLt = nullptr;
              CHECK_NAPI_RESULT(napi_get_property(env, optionsObj, pnLt, &valLt));
              CHECK_NAPI_RESULT(napi_buffer_has_instance(env, valLt, &r));
              CHECK_NAPI_RESULT(napi_get_type_of_value(env, valLt, &t));

              if (r || t == napi_string) {
                  napi_value ltBuffer = valLt;

                  // ignore end if it has size 0 since a Slice can't have length 0
                  if (StringOrBufferLength(env, ltBuffer) > 0) {
                      LD_STRING_OR_BUFFER_TO_COPY(_lt, ltBuffer, lt)
                          lt = new std::string(_ltCh_, _ltSz_);
                      delete[] _ltCh_;
                      if (reverse) {
                          if (startStr != NULL) {
                              delete[] startStr;
                              startStr = NULL;
                          }
                          if (start != NULL)
                              delete start;
                          start = new leveldb::Slice(lt->data(), lt->size());
                      }
                  }
              }
          }

          napi_propertyname pnLte;
          CHECK_NAPI_RESULT(napi_property_name(env, "lte", &pnLte));
          CHECK_NAPI_RESULT(napi_has_property(env, optionsObj, pnLte, &r));
          if (r) {
              napi_value valLte = nullptr;
              CHECK_NAPI_RESULT(napi_get_property(env, optionsObj, pnLte, &valLte));
              CHECK_NAPI_RESULT(napi_buffer_has_instance(env, valLte, &r));
              CHECK_NAPI_RESULT(napi_get_type_of_value(env, valLte, &t));

              if (r || t == napi_string) {
                  napi_value lteBuffer = valLte;

                  // ignore end if it has size 0 since a Slice can't have length 0
                  if (StringOrBufferLength(env, lteBuffer) > 0) {
                      LD_STRING_OR_BUFFER_TO_COPY(_lte, lteBuffer, lte)
                          lte = new std::string(_lteCh_, _lteSz_);
                      delete[] _lteCh_;
                      if (reverse) {
                          if (startStr != NULL) {
                              delete[] startStr;
                              startStr = NULL;
                          }
                          if (start != NULL)
                              delete start;
                          start = new leveldb::Slice(lte->data(), lte->size());
                      }
                  }
              }
          }

          napi_propertyname pnGt;
          CHECK_NAPI_RESULT(napi_property_name(env, "gt", &pnGt));
          CHECK_NAPI_RESULT(napi_has_property(env, optionsObj, pnGt, &r));
          if (r) {
              napi_value valGt = nullptr;
              CHECK_NAPI_RESULT(napi_get_property(env, optionsObj, pnGt, &valGt));
              CHECK_NAPI_RESULT(napi_buffer_has_instance(env, valGt, &r));
              CHECK_NAPI_RESULT(napi_get_type_of_value(env, valGt, &t));

              if (r || t == napi_string) {
                  napi_value gtBuffer = valGt;

                  // ignore end if it has size 0 since a Slice can't have length 0
                  if (StringOrBufferLength(env, gtBuffer) > 0) {
                      LD_STRING_OR_BUFFER_TO_COPY(_gt, gtBuffer, gt)
                          gt = new std::string(_gtCh_, _gtSz_);
                      delete[] _gtCh_;
                      if (!reverse) {
                          if (startStr != NULL) {
                              delete[] startStr;
                              startStr = NULL;
                          }
                          if (start != NULL)
                              delete start;
                          start = new leveldb::Slice(gt->data(), gt->size());
                      }
                  }
              }
          }

          napi_propertyname pnGte;
          CHECK_NAPI_RESULT(napi_property_name(env, "gte", &pnGte));
          CHECK_NAPI_RESULT(napi_has_property(env, optionsObj, pnGte, &r));
          if (r) {
              napi_value valGte = nullptr;
              CHECK_NAPI_RESULT(napi_get_property(env, optionsObj, pnGte, &valGte));
              CHECK_NAPI_RESULT(napi_buffer_has_instance(env, valGte, &r));
              CHECK_NAPI_RESULT(napi_get_type_of_value(env, valGte, &t));

              if (r || t == napi_string) {
                  napi_value gteBuffer = valGte;

                  // ignore end if it has size 0 since a Slice can't have length 0
                  if (StringOrBufferLength(env, gteBuffer) > 0) {
                      LD_STRING_OR_BUFFER_TO_COPY(_gte, gteBuffer, gte)
                          gte = new std::string(_gteCh_, _gteSz_);
                      delete[] _gteCh_;
                      if (!reverse) {
                          if (startStr != NULL) {
                              delete[] startStr;
                              startStr = NULL;
                          }
                          if (start != NULL)
                              delete start;
                          start = new leveldb::Slice(gte->data(), gte->size());
                      }
                  }
              }
          }
      }
  }

  bool keys = BooleanOptionValue(env, optionsObj, "keys", true);
  bool values = BooleanOptionValue(env, optionsObj, "values", true);
  bool keyAsBuffer = BooleanOptionValue(env, optionsObj, "keyAsBuffer", true);
  bool valueAsBuffer = BooleanOptionValue(env, optionsObj, "valueAsBuffer", true);
  bool fillCache = BooleanOptionValue(env, optionsObj, "fillCache");
  int32_t intId;
  CHECK_NAPI_RESULT(napi_get_value_int32(env, id, &intId));

  Iterator* iterator = new Iterator(
      database
    , intId
    , start
    , end
    , reverse
    , keys
    , values
    , limit
    , lt
    , lte
    , gt
    , gte
    , fillCache
    , keyAsBuffer
    , valueAsBuffer
    , highWaterMark
  );

  napi_value _this;
  CHECK_NAPI_RESULT(napi_get_cb_this(env, info, &_this));
  napi_value externalObj;
  CHECK_NAPI_RESULT(napi_make_external(env, _this, &externalObj));
  CHECK_NAPI_RESULT(napi_wrap(env, externalObj, iterator, Iterator::Destructor, &iterator->handle));
  CHECK_NAPI_RESULT(napi_set_return_value(env, info, externalObj));
}

void Iterator::Destructor(void* obj) {
  Iterator* iterator = static_cast<Iterator*>(obj);
  delete iterator;
}

} // namespace leveldown
