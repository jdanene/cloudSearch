package edu.northwestern.ssa.api;

import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;
import edu.northwestern.ssa.AwsSignedRestRequest;
import edu.northwestern.ssa.database.ElasticSearch;
import software.amazon.awssdk.http.HttpExecuteResponse;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import javax.ws.rs.core.MediaType;

@Path("/search")
public class Search {

  ElasticSearch elasticSearch = new ElasticSearch();
  static final String DATE_FORMAT = "YYYY-MM-DD";
  Set<String> ISO_LANGUAGES = new HashSet<>(Arrays.asList(Locale.getISOLanguages()));

  /**
   * Checks a date is properly formatted.
   *
   * @param date string date
   * @return boolean indicating if properly formatted date or not
   * @implNote From https://stackoverflow.com/questions/226910/how-to-sanity-check-a-date-in-java
   */
  public static boolean isDateValid(String date) {
    try {
      DateFormat df = new SimpleDateFormat(DATE_FORMAT);
      df.setLenient(false);
      df.parse(date);
      return true;
    } catch (ParseException e) {
      return false;
    }
  }

  public static String formatPayload(String payload) {
    return null;
  }

  /**
   * http://localhost:8080/api/search?query=Northwestern&count=01
   * http://localhost:8080/api/search?query=Northwestern
   * http://ssa-hw2-backend.stevetarzia.com/api/search?query=Northwestern
   */
  /**
   * when testing, this is reachable at http://localhost:8080/api/search?query="hello bitch"%20hey
   */
  /** http://localhost:8080/api/search?query=hello%20bitch&language=espanol */
  // http://ssa-hw2-backend.stevetarzia.com/api/search?query=Northwestern
  @ApiOperation(value = "Searches AWS elastic search given query parameters")
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "Query Successful"),
        @ApiResponse(code = 404, message = "Required query parameter missing"),
        @ApiResponse(code = 500, message = "Internal Server Error"),
        @ApiResponse(code = 502, message = "Bad gateway"),
        @ApiResponse(code = 406, message = "Invalid value")
      })
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getMsg(
      @QueryParam("query") String queryParam,
      @QueryParam("language") String languageParam,
      @QueryParam("date") String dateParam,
      @QueryParam("count") String countParam,
      @QueryParam("offset") String offsetParam) {

    // Check that required parameter "query" is present
    if (queryParam == null) {
      return Response.status(404)
          .type(MediaType.TEXT_PLAIN)
          .entity("Missing required parameter `query`")
          .header("Access-Control-Allow-Origin", "*")
          .build();
    }

    // Check that date is formatted as "YYYY-MM-DD"
    Optional<String> optionalDateParam = Optional.ofNullable(dateParam);
    if (optionalDateParam.isPresent()) {
      if (!(isDateValid(optionalDateParam.get()))) {
        return Response.status(406)
            .type(MediaType.TEXT_PLAIN)
            .entity("Invalid value: Date")
            .header("Access-Control-Allow-Origin", "*")
            .build();
      }
    }

    // Check that language is of ISO
    Optional<String> optionalLanguageParam = Optional.ofNullable(languageParam);
    if (optionalLanguageParam.isPresent()) {
      if (!(ISO_LANGUAGES.contains(optionalLanguageParam.get()))) {
        return Response.status(406)
            .type(MediaType.TEXT_PLAIN)
            .entity("Invalid value: Language")
            .header("Access-Control-Allow-Origin", "*")
            .build();
      }
    }

    // Check that count parameter is unsigned integer
    Optional<String> optionalCountParam = Optional.ofNullable(countParam);
    if (optionalCountParam.isPresent()) {
      if (!(optionalCountParam.get().matches("^//d+$"))) {
        return Response.status(406)
            .type(MediaType.TEXT_PLAIN)
            .entity("Invalid value: Count")
            .header("Access-Control-Allow-Origin", "*")
            .build();
      }
    }

    // Check that offset parameter is unsigned integer
    Optional<String> optionalOffsetParam = Optional.ofNullable(offsetParam);
    if (optionalOffsetParam.isPresent()) {
      if (!(optionalOffsetParam.get().matches("^//d+$"))) {
        return Response.status(406)
            .type(MediaType.TEXT_PLAIN)
            .entity("Invalid value: Offset")
            .header("Access-Control-Allow-Origin", "*")
            .build();
      }
    }

    // Post  query to ElasticSearch
    ElasticSearch elasticSearch = new ElasticSearch();
    HttpExecuteResponse httpExecuteResponse = null;
    try {
      httpExecuteResponse =
          elasticSearch.queryElasticSearch(
              queryParam,
              optionalLanguageParam,
              optionalDateParam,
              optionalCountParam,
              optionalOffsetParam);
    } catch (IOException e) {
      return Response.status(502)
          .type(MediaType.TEXT_PLAIN)
          .entity("Bad gateway")
          .header("Access-Control-Allow-Origin", "*")
          .build();
    }

    // Get the body of the response
    String requestBody;
    try {
      requestBody =
          ElasticSearch.formatPayload(AwsSignedRestRequest.getResponseBody(httpExecuteResponse));

    } catch (IOException e) {
      return Response.status(500)
          .type(MediaType.TEXT_PLAIN)
          .entity("Internal Server Error")
          .header("Access-Control-Allow-Origin", "*")
          .build();
    }

    // get statusCode of response
    int statusCode = httpExecuteResponse.httpResponse().statusCode();

    return Response.status(statusCode)
        .type(MediaType.APPLICATION_JSON)
        .entity(requestBody)
        .header("Access-Control-Allow-Origin", "*")
        .build();
  }
}
