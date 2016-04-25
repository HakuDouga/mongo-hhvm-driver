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

#include "pool.h"

extern "C" {
#include "../../../libmongoc/src/mongoc/mongoc-client.h"
}

namespace {
	thread_local std::unordered_map<std::string, std::shared_ptr<HPHP::Pool>> s_connections;
}

namespace HPHP {

std::string Pool::GetHash(const mongoc_uri_t *uri)
{
	return mongoc_uri_get_string(uri);
}

mongoc_client_t *Pool::GetClient(const mongoc_uri_t *uri)
{
	auto key = GetHash(uri);
	std::shared_ptr<Pool> tmp;

	tmp = s_connections[key];
	if (!tmp) {
		std::shared_ptr<Pool> new_pool_ptr(new Pool(uri));
		s_connections[key] = new_pool_ptr;
		return mongoc_client_pool_pop(new_pool_ptr->m_pool);
	} else {
		return mongoc_client_pool_pop(tmp->m_pool);
	}
}

void Pool::ReturnClient(const mongoc_uri_t *uri, mongoc_client_t *client)
{
	auto key = GetHash(uri);
	std::shared_ptr<Pool> tmp;

	tmp = s_connections[key];
	if (!tmp) {
std::cout << "THE POOL IS GONE\n";
	} else {
		mongoc_client_pool_push(tmp->m_pool, client);
	}
}

}
