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
    Nan::AsyncQueueWorker(iterator->endWorker);
    iterator->endWorker = NULL;
  }
}

NAPI_METHOD(Iterator::Seek) {
  napi_value args[1];
  napi_get_cb_args(env, info, args, 1);
  napi_value thisObj = napi_get_cb_this(env, info);
  Iterator* iterator = static_cast<Iterator*>(napi_unwrap(env, thisObj));
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

  napi_value holder = napi_get_cb_holder(env, info);
  napi_set_return_value(env, info, holder);
}

NAPI_METHOD(Iterator::Next) {
  napi_value args[1];
  napi_get_cb_args(env, info, args, 1);
  napi_value thisObj = napi_get_cb_this(env, info);
  Iterator* iterator = static_cast<Iterator*>(napi_unwrap(env, thisObj));

  if (napi_get_type_of_value(env, args[0]) != napi_function) {
    return Nan::ThrowError("next() requires a callback argument");
  }

  napi_value callback = args[0];

  NextWorker* worker = new NextWorker(
      iterator
    , callback
    , checkEndCallback
  );
  // persist to prevent accidental GC
  v8::Local<v8::Object> _this = V8LocalValue(thisObj).As<v8::Object>();
  worker->SaveToPersistent("iterator", _this);
  iterator->nexting = true;
  Nan::AsyncQueueWorker(worker);

  napi_value holder = napi_get_cb_holder(env, info);
  napi_set_return_value(env, info, holder);
}

NAPI_METHOD(Iterator::End) {
  napi_value args[1];
  napi_get_cb_args(env, info, args, 1);
  napi_value thisObj = napi_get_cb_this(env, info);
  Iterator* iterator = static_cast<Iterator*>(napi_unwrap(env, thisObj));

  if (napi_get_type_of_value(env, args[0]) != napi_function) {
    return Nan::ThrowError("end() requires a callback argument");
  }

  if (!iterator->ended) {
    napi_value callback = args[0];

    EndWorker* worker = new EndWorker(
        iterator
      , callback
    );
    // persist to prevent accidental GC
    v8::Local<v8::Object> _this = V8LocalValue(thisObj).As<v8::Object>();
    worker->SaveToPersistent("iterator", _this);
    iterator->ended = true;

    if (iterator->nexting) {
      // waiting for a next() to return, queue the end
      iterator->endWorker = worker;
    } else {
      Nan::AsyncQueueWorker(worker);
    }
  }

  napi_value holder = napi_get_cb_holder(env, info);
  napi_set_return_value(env, info, holder);
}

void Iterator::Init (napi_env env) {
  napi_value ctor = napi_create_constructor_for_wrap(env, Iterator::New);
  napi_set_function_name(env, ctor, napi_property_name(env, "Iterator"));

  // Was this never used?
  //tpl->InstanceTemplate()->SetInternalFieldCount(1);

  napi_value proto = napi_get_property(env, ctor, napi_property_name(env, "prototype"));

  napi_value fnSeek = napi_create_function(env, Iterator::Seek);
  napi_propertyname pnSeek = napi_property_name(env, "seek");
  napi_set_function_name(env, fnSeek, pnSeek);
  napi_set_property(env, proto, pnSeek, fnSeek);

  napi_value fnNext = napi_create_function(env, Iterator::Next);
  napi_propertyname pnNext = napi_property_name(env, "next");
  napi_set_function_name(env, fnNext, pnNext);
  napi_set_property(env, proto, pnNext, fnNext);

  napi_value fnEnd = napi_create_function(env, Iterator::End);
  napi_propertyname pnEnd = napi_property_name(env, "end");
  napi_set_function_name(env, fnEnd, pnEnd);
  napi_set_property(env, proto, pnEnd, fnEnd);

  iterator_constructor = napi_create_persistent(env, ctor);
}

napi_value Iterator::NewInstance (
        napi_env env
      , napi_value database
      , napi_value id
      , napi_value optionsObj
    ) {

  Napi::EscapableHandleScope scope(env);

  napi_value instance;
  napi_value constructorHandle = napi_get_persistent_value(env, iterator_constructor);

  if (optionsObj == nullptr) {
    napi_value argv[2] = { database, id };
    instance = napi_new_instance(env, constructorHandle, 2, argv);
  } else {
    napi_value argv[3] = { database, id, optionsObj };
    instance = napi_new_instance(env, constructorHandle, 3, argv);
  }

  return scope.Escape(instance);
}

NAPI_METHOD(Iterator::New) {
  napi_value args[3];
  napi_get_cb_args(env, info, args, 3);
  int argsLength = napi_get_cb_args_length(env, info);

  Database* database = static_cast<Database*>(napi_unwrap(env, args[0]));

  leveldb::Slice* start = NULL;
  std::string* end = NULL;
  int limit = -1;
  // default highWaterMark from Readble-streams
  size_t highWaterMark = 16 * 1024;

  napi_value id = args[1];

  napi_value optionsObj = nullptr;

  v8::Local<v8::Object> ltHandle;
  v8::Local<v8::Object> lteHandle;
  v8::Local<v8::Object> gtHandle;
  v8::Local<v8::Object> gteHandle;

  char *startStr = NULL;
  std::string* lt = NULL;
  std::string* lte = NULL;
  std::string* gt = NULL;
  std::string* gte = NULL;

  //default to forward.
  bool reverse = false;

  if (argsLength > 1 && napi_get_type_of_value(env, args[2]) == napi_object) {
    optionsObj = args[2];

    reverse = BooleanOptionValue(env, optionsObj, "reverse");

    napi_propertyname pnStart = napi_property_name(env, "start");
    napi_value valStart = nullptr;
    if (napi_has_property(env, optionsObj, pnStart)
        && (node::Buffer::HasInstance(V8LocalValue(valStart = napi_get_property(env, optionsObj, pnStart)))
          || napi_get_type_of_value(env, valStart) == napi_string)) {

      v8::Local<v8::Value> startBuffer = V8LocalValue(valStart);

      // ignore start if it has size 0 since a Slice can't have length 0
      if (StringOrBufferLength(startBuffer) > 0) {
        LD_STRING_OR_BUFFER_TO_COPY(_start, startBuffer, start)
        start = new leveldb::Slice(_startCh_, _startSz_);
        startStr = _startCh_;
      }
    }

    napi_propertyname pnEnd = napi_property_name(env, "end");
    napi_value valEnd = nullptr;
    if (napi_has_property(env, optionsObj, pnEnd)
        && (node::Buffer::HasInstance(V8LocalValue(valEnd = napi_get_property(env, optionsObj, pnEnd)))
          || napi_get_type_of_value(env, valEnd) == napi_string)) {

      v8::Local<v8::Value> endBuffer = V8LocalValue(valEnd);

      // ignore end if it has size 0 since a Slice can't have length 0
      if (StringOrBufferLength(endBuffer) > 0) {
        LD_STRING_OR_BUFFER_TO_COPY(_end, endBuffer, end)
        end = new std::string(_endCh_, _endSz_);
        delete[] _endCh_;
      }
    }

    napi_propertyname pnLimit = napi_property_name(env, "limit");
    if (optionsObj != nullptr && napi_has_property(env, optionsObj, pnLimit)) {
      limit = napi_get_value_int64(env, napi_get_property(env, optionsObj, pnLimit));
    }

    napi_propertyname pnHighWaterMark = napi_property_name(env, "highWaterMark");
    if (napi_has_property(env, optionsObj, pnHighWaterMark)) {
      highWaterMark = napi_get_value_int64(env, napi_get_property(env, optionsObj, pnHighWaterMark));
    }

    napi_propertyname pnLt = napi_property_name(env, "lt");
    napi_value valLt = nullptr;
    if (napi_has_property(env, optionsObj, pnLt)
        && (node::Buffer::HasInstance(V8LocalValue(valLt = napi_get_property(env, optionsObj, pnLt)))
          || napi_get_type_of_value(env, valLt) == napi_string)) {

      v8::Local<v8::Value> ltBuffer = V8LocalValue(valLt);

      // ignore end if it has size 0 since a Slice can't have length 0
      if (StringOrBufferLength(ltBuffer) > 0) {
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

    napi_propertyname pnLte = napi_property_name(env, "lte");
    napi_value valLte = nullptr;
    if (napi_has_property(env, optionsObj, pnLte)
        && (node::Buffer::HasInstance(V8LocalValue(valLte = napi_get_property(env, optionsObj, pnLte)))
          || napi_get_type_of_value(env, valLte) == napi_string)) {

      v8::Local<v8::Value> lteBuffer = V8LocalValue(valLte);

      // ignore end if it has size 0 since a Slice can't have length 0
      if (StringOrBufferLength(lteBuffer) > 0) {
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

    napi_propertyname pnGt = napi_property_name(env, "gt");
    napi_value valGt = nullptr;
    if (napi_has_property(env, optionsObj, pnGt)
        && (node::Buffer::HasInstance(V8LocalValue(valGt = napi_get_property(env, optionsObj, pnGt)))
          || napi_get_type_of_value(env, valGt) == napi_string)) {

      v8::Local<v8::Value> gtBuffer = V8LocalValue(valGt);

      // ignore end if it has size 0 since a Slice can't have length 0
      if (StringOrBufferLength(gtBuffer) > 0) {
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

    napi_propertyname pnGte = napi_property_name(env, "gte");
    napi_value valGte = nullptr;
    if (napi_has_property(env, optionsObj, pnGte)
        && (node::Buffer::HasInstance(V8LocalValue(valGte = napi_get_property(env, optionsObj, pnGte)))
          || napi_get_type_of_value(env, valGte) == napi_string)) {

      v8::Local<v8::Value> gteBuffer = V8LocalValue(valGte);

      // ignore end if it has size 0 since a Slice can't have length 0
      if (StringOrBufferLength(gteBuffer) > 0) {
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

  bool keys = BooleanOptionValue(env, optionsObj, "keys", true);
  bool values = BooleanOptionValue(env, optionsObj, "values", true);
  bool keyAsBuffer = BooleanOptionValue(env, optionsObj, "keyAsBuffer", true);
  bool valueAsBuffer = BooleanOptionValue(env, optionsObj, "valueAsBuffer", true);
  bool fillCache = BooleanOptionValue(env, optionsObj, "fillCache");

  Iterator* iterator = new Iterator(
      database
    , (uint32_t)napi_get_value_int32(env, id)
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
  napi_value thisObj = napi_get_cb_this(env, info);
  napi_wrap(env, thisObj, iterator, Iterator::Destructor, &iterator->handle);

  napi_set_return_value(env, info, thisObj);
}

void Iterator::Destructor(void* obj) {
  Iterator* iterator = static_cast<Iterator*>(obj);
  delete iterator;
}

} // namespace leveldown
