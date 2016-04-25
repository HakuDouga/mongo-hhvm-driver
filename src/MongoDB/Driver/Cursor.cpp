/**
 *  Copyright 2014-2015 MongoDB, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

#include "hphp/runtime/ext/extension.h"
#include "hphp/runtime/vm/native-data.h"

#include "../../../bson.h"
#include "../../../utils.h"
#include "../../../mongodb.h"

#include "CursorId.h"
#include "Cursor.h"
#include "Server.h"

namespace HPHP {

const StaticString s_MongoDriverCursor_className("MongoDB\\Driver\\Cursor");
Class* MongoDBDriverCursorData::s_class = nullptr;
const StaticString MongoDBDriverCursorData::s_className("MongoDBDriverCursor");
const StaticString s_MongoDBDriverCursor_cursor("cursor");
const StaticString s_MongoDBDriverCursor_stamp("stamp");
const StaticString s_MongoDBDriverCursor_is_command("is_command");
const StaticString s_MongoDBDriverCursor_sent("sent");
const StaticString s_MongoDBDriverCursor_done("done");
const StaticString s_MongoDBDriverCursor_failed("failed");
const StaticString s_MongoDBDriverCursor_end_of_event("end_of_event");
const StaticString s_MongoDBDriverCursor_in_exhaust("in_exhaust");
const StaticString s_MongoDBDriverCursor_redir_primary("redir_primary");
const StaticString s_MongoDBDriverCursor_has_fields("has_fields");
const StaticString s_MongoDBDriverCursor_query("query");
const StaticString s_MongoDBDriverCursor_fields("fields");
const StaticString s_MongoDBDriverCursor_read_preference("read_preference");
const StaticString s_MongoDBDriverCursor_read_preference_mode("mode");
const StaticString s_MongoDBDriverCursor_read_preference_tags("tags");
const StaticString s_MongoDBDriverCursor_flags("flags");
const StaticString s_MongoDBDriverCursor_skip("skip");
const StaticString s_MongoDBDriverCursor_limit("limit");
const StaticString s_MongoDBDriverCursor_count("count");
const StaticString s_MongoDBDriverCursor_batch_size("batch_size");
const StaticString s_MongoDBDriverCursor_ns("ns");
const StaticString s_MongoDBDriverCursor_current_doc("current_doc");
const StaticString s_MongoDBDriverCursor_server_id("server_id");
IMPLEMENT_GET_CLASS(MongoDBDriverCursorData);

static bool hippo_cursor_load_current(MongoDBDriverCursorData* data);

Object hippo_cursor_init(mongoc_cursor_t *cursor, mongoc_client_t *client)
{
	static HPHP::Class* c_result;

	c_result = HPHP::Unit::lookupClass(HPHP::s_MongoDriverCursor_className.get());
	assert(c_result);
	HPHP::Object obj = HPHP::Object{c_result};

	HPHP::MongoDBDriverCursorData* cursor_data = HPHP::Native::data<HPHP::MongoDBDriverCursorData>(obj.get());

	cursor_data->cursor = cursor;
	cursor_data->client = client;
	cursor_data->m_server_id = mongoc_cursor_get_hint(cursor);
	cursor_data->next_after_rewind = 0;

	hippo_cursor_load_current(cursor_data);

	return obj;
}

static void invalidate_current(MongoDBDriverCursorData *data)
{
	if (data->zchild_active) {
		data->zchild_active = false;
	}
}

Array HHVM_METHOD(MongoDBDriverCursor, __debugInfo)
{
	MongoDBDriverCursorData* data = Native::data<MongoDBDriverCursorData>(this_);
	Array retval = Array::Create();

	if (data->cursor) {
		Array cretval = Array::Create();
		const bson_t *tags = mongoc_read_prefs_get_tags(data->cursor->read_prefs);

		cretval.add(s_MongoDBDriverCursor_stamp, (int64_t) data->cursor->stamp);
		cretval.add(s_MongoDBDriverCursor_is_command, !!(data->cursor->is_command));
		cretval.add(s_MongoDBDriverCursor_sent, !!data->cursor->sent);
		cretval.add(s_MongoDBDriverCursor_done, !!data->cursor->done);
		cretval.add(s_MongoDBDriverCursor_end_of_event, !!data->cursor->end_of_event);
		cretval.add(s_MongoDBDriverCursor_in_exhaust, !!data->cursor->in_exhaust);
		cretval.add(s_MongoDBDriverCursor_has_fields, !!data->cursor->has_fields);

		{
			Variant v_query;

			hippo_bson_conversion_options_t options = HIPPO_TYPEMAP_INITIALIZER;
			BsonToVariantConverter convertor(bson_get_data(&data->cursor->query), data->cursor->query.len, options);
			convertor.convert(&v_query);
			cretval.add(s_MongoDBDriverCursor_query, v_query);
		}
		{
			Variant v_fields;

			hippo_bson_conversion_options_t options = HIPPO_TYPEMAP_INITIALIZER;
			BsonToVariantConverter convertor(bson_get_data(&data->cursor->fields), data->cursor->fields.len, options);
			convertor.convert(&v_fields);
			cretval.add(s_MongoDBDriverCursor_fields, v_fields);
		}
		if (tags->len) {
			Array rp_retval = Array::Create();
			Variant v_read_preference;

			rp_retval.add(s_MongoDBDriverCursor_read_preference_mode, (int64_t) mongoc_read_prefs_get_mode(data->cursor->read_prefs));

			hippo_bson_conversion_options_t options = HIPPO_TYPEMAP_DEBUG_INITIALIZER;
			BsonToVariantConverter convertor(bson_get_data(tags), tags->len, options);
			convertor.convert(&v_read_preference);
			rp_retval.add(s_MongoDBDriverCursor_read_preference_tags, v_read_preference);

			cretval.add(s_MongoDBDriverCursor_read_preference, rp_retval);
		} else {
			cretval.add(s_MongoDBDriverCursor_read_preference, Variant());
		}

		cretval.add(s_MongoDBDriverCursor_flags, (int64_t) data->cursor->flags);
		cretval.add(s_MongoDBDriverCursor_skip, (int64_t) data->cursor->skip);
		cretval.add(s_MongoDBDriverCursor_limit, (int64_t) data->cursor->limit);
		cretval.add(s_MongoDBDriverCursor_count, (int64_t) data->cursor->count);
		cretval.add(s_MongoDBDriverCursor_batch_size, (int64_t) data->cursor->batch_size);

		cretval.add(s_MongoDBDriverCursor_ns, data->cursor->ns);

		if (data->cursor->current) {
			Array doc_retval = Array::Create();
			Variant v_doc;

			hippo_bson_conversion_options_t options = HIPPO_TYPEMAP_INITIALIZER;
			BsonToVariantConverter convertor(bson_get_data(data->cursor->current), data->cursor->current->len, options);
			convertor.convert(&v_doc);
			cretval.add(s_MongoDBDriverCursor_current_doc, v_doc);
		}

		retval.add(s_MongoDBDriverCursor_cursor, cretval);
	} else {
		retval.add(s_MongoDBDriverCursor_cursor, Variant());
	}

	retval.add(s_MongoDBDriverCursor_server_id, data->m_server_id);

	return retval;
}


Object HHVM_METHOD(MongoDBDriverCursor, getId)
{
	static Class* c_cursor;
	int64_t cursorid;
	MongoDBDriverCursorData* data = Native::data<MongoDBDriverCursorData>(this_);

	cursorid = mongoc_cursor_get_id(data->cursor);

	/* Prepare result */
	c_cursor = Unit::lookupClass(s_MongoDriverCursorId_className.get());
	assert(c_cursor);
	Object obj = Object{c_cursor};

	MongoDBDriverCursorIdData* cursorid_data = Native::data<MongoDBDriverCursorIdData>(obj.get());

	cursorid_data->id = cursorid;

	return obj;
}

Variant HHVM_METHOD(MongoDBDriverCursor, current)
{
	MongoDBDriverCursorData* data = Native::data<MongoDBDriverCursorData>(this_);

	return data->zchild;
}

int64_t HHVM_METHOD(MongoDBDriverCursor, key)
{
	MongoDBDriverCursorData* data = Native::data<MongoDBDriverCursorData>(this_);

	return data->current;
}

bool hippo_cursor_load_current(MongoDBDriverCursorData* data)
{
	const bson_t *doc;

	invalidate_current(data);

	doc = mongoc_cursor_current(data->cursor);
	if (doc) {
		Variant v;

		BsonToVariantConverter convertor(bson_get_data(doc), doc->len, data->bson_options);
		convertor.convert(&v);
		data->zchild_active = true;
		data->zchild = v;

		return true;
	}
	return false;
}

static bool hippo_cursor_load_next(MongoDBDriverCursorData* data)
{
	const bson_t *doc;

	if (mongoc_cursor_next(data->cursor, &doc)) {
		return hippo_cursor_load_current(data);
	}
	return false;
}

static bool hippo_cursor_next(MongoDBDriverCursorData* data)
{
	invalidate_current(data);

	data->next_after_rewind++;

	data->current++;
	if (hippo_cursor_load_next(data)) {
		return true;
	} else {
		invalidate_current(data);
	}

	return false;
}

Variant HHVM_METHOD(MongoDBDriverCursor, next)
{
	MongoDBDriverCursorData* data = Native::data<MongoDBDriverCursorData>(this_);

	return hippo_cursor_next(data) ? String("next") : String("fail");
}

static void hippo_cursor_rewind(MongoDBDriverCursorData* data)
{
	if (data->next_after_rewind != 0) {
		if (data->zchild_active) {
			throw MongoDriver::Utils::throwLogicException("Cursors cannot rewind after starting iteration");
		} else { /* If we're not active, image we're now have fully iterated */
			throw MongoDriver::Utils::throwLogicException("Cursors cannot yield multiple iterators");
		}
	}

	data->current = 0;
}

void HHVM_METHOD(MongoDBDriverCursor, rewind)
{
	MongoDBDriverCursorData* data = Native::data<MongoDBDriverCursorData>(this_);

	hippo_cursor_rewind(data);
}

bool HHVM_METHOD(MongoDBDriverCursor, valid)
{
	MongoDBDriverCursorData* data = Native::data<MongoDBDriverCursorData>(this_);

	if (data->zchild_active) {
		return true;
	}

	return false;
}

bool HHVM_METHOD(MongoDBDriverCursor, isDead)
{
	MongoDBDriverCursorData* data = Native::data<MongoDBDriverCursorData>(this_);

	return !(mongoc_cursor_is_alive(data->cursor));
}

Array HHVM_METHOD(MongoDBDriverCursor, toArray)
{
	MongoDBDriverCursorData* data = Native::data<MongoDBDriverCursorData>(this_);
	Array retval = Array::Create();

	hippo_cursor_rewind(data);

	while (data->zchild_active) {
		retval.add(data->current, data->zchild);
		hippo_cursor_next(data);
	}

	return retval;
}

Object HHVM_METHOD(MongoDBDriverCursor, getServer)
{
	MongoDBDriverCursorData* data = Native::data<MongoDBDriverCursorData>(this_);

	return hippo_mongo_driver_server_create_from_id(data->client, data->m_server_id);
}


void HHVM_METHOD(MongoDBDriverCursor, setTypeMap, const Array &typemap)
{
	MongoDBDriverCursorData* data = Native::data<MongoDBDriverCursorData>(this_);

	parseTypeMap(&data->bson_options, typemap);

	/* Reconvert the current document to use the new typemap */
	hippo_cursor_load_current(data);
}

}
