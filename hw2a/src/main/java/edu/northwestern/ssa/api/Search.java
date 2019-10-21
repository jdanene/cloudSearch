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
  static final String DATE_FORMAT = "yyyy-MM-dd";
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
        @ApiResponse(code = 400, message = "Required query parameter missing"),
        @ApiResponse(code = 500, message = "Internal Server Error"),
        @ApiResponse(code = 502, message = "Bad gateway"),
        @ApiResponse(code = 406, message = "Invalid value for key")
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
      return Response.status(400)
          .type(MediaType.TEXT_PLAIN)
          .entity("Missing required parameter `query`")
          .header("Access-Control-Allow-Origin", "*")
          .build();
    }

    // Check that date is formatted as "YYYY-MM-DD"
    if (dateParam!=null) {
      if (!(isDateValid(dateParam))) {
        return Response.status(406)
            .type(MediaType.TEXT_PLAIN)
            .entity("Invalid value for key: `date`")
            .header("Access-Control-Allow-Origin", "*")
            .build();
      }
    }

    // Check that language is of ISO
    if (languageParam != null) {
      if (!(ISO_LANGUAGES.contains(languageParam))) {
        return Response.status(406)
            .type(MediaType.TEXT_PLAIN)
            .entity("Invalid value for key: `language`, not a ISO Language Code")
            .header("Access-Control-Allow-Origin", "*")
            .build();
      }
    }

    // Check that count parameter is unsigned integer
    if (countParam != null) {
      if (!(countParam.matches("^[0-9]+"))) {
          return Response.status(406)
            .type(MediaType.TEXT_PLAIN)
            .entity("Invalid value for key: `count`")
            .header("Access-Control-Allow-Origin", "*")
            .build();
      }
    }

    // Check that offset parameter is unsigned integer
    if (offsetParam!=null) {
      if (!(offsetParam.matches("^[0-9]+"))) {
        return Response.status(406)
            .type(MediaType.TEXT_PLAIN)
            .entity("Invalid value key: `offset`")
            .header("Access-Control-Allow-Origin", "*")
            .build();
      }
    }

      Optional<String> optionalDateParam = Optional.ofNullable(dateParam);
      Optional<String> optionalOffsetParam = Optional.ofNullable(offsetParam);
      Optional<String> optionalCountParam = Optional.ofNullable(countParam);
      Optional<String> optionalLanguageParam = Optional.ofNullable(languageParam);


      // Post  query to ElasticSearch
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
      requestBody = AwsSignedRestRequest.getResponseBody(httpExecuteResponse);
      System.out.println(requestBody);
        requestBody = ElasticSearch.formatPayload(requestBody);

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
