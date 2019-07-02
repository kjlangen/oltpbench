/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/

package com.oltpbenchmark.benchmarks.wikipedia.util;

import java.util.Map;
import java.util.List;
import java.util.HashMap;

import org.apache.log4j.Logger;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.WebResource.Builder;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.type.TypeReference;

import org.apache.log4j.Logger;

public class RestQuery
{
	private static final Logger LOG = Logger.getLogger(RestQuery.class);

	public static List<Map<String, Object>> restReadQuery(String queryString, int clientId)
	{
		// Make a new client pointing at Apollo/the rest service
		Client client = new Client();
		String target = "http://3.91.230.74:8080/kronos/rest/query/" + clientId;
        WebResource resource = client.resource(target);

        // Make the post query
        LOG.info(String.format("Here in restQuery before response."));
        String response = resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).post(String.class, queryString);
        LOG.info(String.format("Here in restQuery after response."));
        LOG.info(String.format("Response was: %s", response));
        
        // Deparse the result
        ObjectMapper mapper = new ObjectMapper();
        List<Map<String, Object>> data = null;
        try
        {
			data = mapper.readValue(response, new TypeReference<List<Map<String, Object>>>(){});
		}
		catch (Exception e)
		{
			LOG.error(String.format( "IOException caught, message: {}", e.getMessage()));
		}

		return data;
	}

	public static int restOtherQuery(String queryString, int clientId)
	{
		// Make a new client pointing at Apollo/the rest service
		Client client = new Client();
		String target = "http://3.91.230.74:8080/kronos/rest/query/" + clientId;
        WebResource resource = client.resource(target);

        // Make the post query
        LOG.info(String.format("Here in restQuery before response."));
        String response = resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).post(String.class, queryString);
        LOG.info(String.format("Here in restQuery after response."));
        LOG.info(String.format("Response was: %s", response));
        
        // Deparse the result
        ObjectMapper mapper = new ObjectMapper();
        int data = -1;
        try
        {
			data = mapper.readValue(response, Integer.class);
		}
		catch (Exception e)
		{
			LOG.error(String.format( "IOException caught, message: {}", e.getMessage()));
		}

		return data;
	}
}
