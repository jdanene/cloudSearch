package edu.northwestern.ssa.database;


import edu.northwestern.ssa.AwsSignedRestRequest;
import edu.northwestern.ssa.Config;
import org.json.JSONArray;
import org.json.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpMethod;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.*;

public class ElasticSearch extends AwsSignedRestRequest {
  static final Logger LOG = LoggerFactory.getLogger(ElasticSearch.class);
  private static final String DEFAULT_COUNT = "10";

  // "es" for Elasticsearch
  private static final String SERVICE_NAME = "es";
  private static final String HOST_NAME = Config.getParam("ELASTIC_SEARCH_HOST");
  private static final String INDEX = Config.getParam("ELASTIC_SEARCH_INDEX");

  public ElasticSearch() {
    super(SERVICE_NAME);
  }

  /**
   * Returns list of all available active indices on the ElasticSearch
   *
   * @return HttpExecuteResponse
   * @throws IOException
   */
  String listActiveIndices() throws IOException {

    HashMap<String, String> queryMap = new HashMap<>();
    queryMap.put("v", "true");
    Optional<Map<String, String>> optionalQueryMap = Optional.of(queryMap);

    String path = "/_cat/indices";
    HttpExecuteResponse httpExecuteResponse =
        restRequest(SdkHttpMethod.GET, HOST_NAME, path, Optional.empty(), Optional.empty());
    return getResponseBody(
        restRequest(SdkHttpMethod.GET, HOST_NAME, path, optionalQueryMap, Optional.empty()));
  }

  /**
   * Takes in a string of query separated by spaces and tokenizes the string into a collection based
   * on spaces or %20.
   *
   * @param queryParam
   * @return
   */
  private static String parseQuery(String queryParam) {
    return "(" + queryParam.replace(" ", " AND ") + ")";
  }

  /**
   * Checks if a json key is missing or does not have a value.
   *
   * @param jsonObject
   * @param key
   * @return
   */
  public static boolean isNullOrEmpty(JSONObject jsonObject, String key) {
    return !(jsonObject.has(key)) || (jsonObject.isNull(key));
  }

  /**
   * Gets the total value from a elastic "hits" objects
   *
   * @param elasticHits
   * @return Example input >> "hits": {"total":{"value":8072,"relation":"eq"}}
   */
  private static Integer getTotalResults(JSONObject elasticHits) {
    JSONObject totalHitsObj = (JSONObject) elasticHits.get("total");
    return totalHitsObj.getInt("value");
  }

  /**
   * Formats an elasticRecord
   *
   * @param elasticRecord
   * @return { "title": STRING, "url": STRING, "txt": STRING, "date": STRING, # null or missing if
   *     not available "lang": STRING # null or missing if not available }
   */
  private static JSONObject getFormattedRecord(JSONObject elasticRecord) {
    JSONObject data = elasticRecord.getJSONObject("_source");

    data.put("txt", data.getString("txt"));
    data.put("title", data.getString("title"));
    data.put("url", data.getString("url"));

    if (!(isNullOrEmpty(data, "lang"))) data.put("lang",data.getString("lang"));

    if (!(isNullOrEmpty(data, "date"))) data.put("date", (String) data.getString("date"));

    return data;
  }

  public static String formatPayload(String payload){
    JSONObject json = new JSONObject(payload);
    JSONObject data = new JSONObject();

    if (isNullOrEmpty(json, "hits")) {
      return data.toString();
    } else {

      // Get the jsonObject of hits
      JSONObject jsonHits = json.getJSONObject("hits");
      // Get the jsonArray of records
      JSONArray jsonHitsArray = jsonHits.getJSONArray("hits");

      // loop through jsonHitsArray and store formatted elements in a JSONArray
      JSONArray articlesArray = new JSONArray();
      for (int i = 0; i < jsonHitsArray.length(); i++) {
        JSONObject elasticRecord = jsonHitsArray.getJSONObject(i);
        articlesArray.put(getFormattedRecord(elasticRecord));
      }
      // store the total number of results on ElasticSearch for the query
      int totalResults = getTotalResults(jsonHits);
      // store the total number of results returned in this query
      int returnedResults = jsonHitsArray.length();

      data.put("returned_results", returnedResults);
      data.put("total_results", totalResults);
      data.put("articles", articlesArray);
      return data.toString(4);
    }
  }

  /**
   * Returns an ElasticSearch response
   *
   * @param queryParam
   * @param languageParam
   * @param dateParam
   * @return
   */
  public HttpExecuteResponse queryElasticSearch(
      String queryParam,
      Optional<String> languageParam,
      Optional<String> dateParam,
      Optional<String> countParam,
      Optional<String> offsetParam)
      throws IOException {

    String luceneQuery = String.format("txt:%s", parseQuery(queryParam));

    if (languageParam.isPresent()){
      luceneQuery =luceneQuery+" AND lang:" + languageParam.get();
    }

    if (dateParam.isPresent()){
      luceneQuery = luceneQuery+" AND date:" + dateParam.get() ;

    }
    System.out.println(dateParam.isPresent());

    HashMap<String, String> queryMap = new HashMap<>();
    queryMap.put("q", luceneQuery);
    queryMap.put("size", countParam.orElse(DEFAULT_COUNT));
    offsetParam.ifPresent(s -> queryMap.put("from", s));
    String path = String.format("/%s/_search", INDEX);

    return restRequest(SdkHttpMethod.GET, HOST_NAME, path, Optional.of(queryMap), Optional.empty());
  }
}
