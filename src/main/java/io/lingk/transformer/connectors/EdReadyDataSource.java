package io.lingk.transformer.connectors;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.http.HttpMethods;
import com.google.api.client.http.HttpStatusCodes;
import io.lingk.transformer.http.HttpResult;
import io.lingk.transformer.inputevent.Params;
import io.lingk.transformer.json.JsonUtil;
import io.lingk.transformer.model.Environment;
import io.lingk.transformer.parse.SparkSchema;
import io.lingk.transformer.provider.DataSource;
import io.lingk.transformer.provider.exception.DataSourceReadException;
import io.lingk.transformer.provider.exception.ProviderConfigurationException;
import io.lingk.transformer.util.RDDUtil;
import io.lingk.transformer.util.TemplateUtil;
import io.lingk.transformer.util.UrlEncodeHelper;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.javatuples.Pair;
import org.json.JSONObject;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static io.lingk.transformer.Constants.ACCEPT;
import static io.lingk.transformer.Constants.BODY_PROPERTY;
import static io.lingk.transformer.connectors.Constants.*;
import static org.apache.http.HttpHeaders.AUTHORIZATION;

@Log4j2
public class EdReadyDataSource extends DataSource {

    public static final String ED_READY_READER = "edReadyReader";

    /* GLOBAL VARIABLES */
    private static final String AUTH_URL = "https://tx.apiedready.edready.org/api/v2/tokens/admin";

    private CloseableHttpClient closeableHttpClient;

    private String url = "";
    private String token = "";
    private String superToken = "";
    private String headers = "";
    private String pagination = "";
    private String httpMethod = "";
    private String body = "";

    public EdReadyDataSource(Map<String, String> config, Params params, Map<String, Object> localContext, String parameterizedBy, int parameterizedByConcurrentThreads, String onError, Map<String, Object> executionContext, Environment environment, SparkSchema sparkSchema, String connectionKey) throws ProviderConfigurationException {
        super(config, params.getEventData(), parameterizedBy, parameterizedByConcurrentThreads, executionContext, environment, sparkSchema);
        log.info("Inside EdReadyDataSource");

        this.superToken = TemplateUtil.optionalProviderProperty(AUTH_TOKEN, this.config, params.getEventData(), executionContext, this.environment);
        this.url = TemplateUtil.optionalProviderProperty(PATH_PROPERTY, this.config, params.getEventData(), executionContext, this.environment);
        this.headers = TemplateUtil.optionalProviderProperty(HEADER_PROPERTY, this.config, params.getEventData(), executionContext, this.environment);
        this.pagination = TemplateUtil.optionalProviderProperty(PAGINATION_PROPERTY, this.config, params.getEventData(), executionContext, this.environment);
        this.httpMethod = TemplateUtil.optionalProviderProperty(HTTP_METHOD_PROPERTY, this.config, params.getEventData(), executionContext, this.environment);
        this.body = TemplateUtil.optionalProviderProperty(BODY_PROPERTY, this.config, params.getEventData(), executionContext, this.environment);

        if (StringUtils.isEmpty(httpMethod)) {
            throw new ProviderConfigurationException("Missing required field httpMethod");
        }
        if (StringUtils.isEmpty(url)) {
            throw new ProviderConfigurationException("Missing required field url");
        }
        if (StringUtils.isEmpty(superToken)) {
            throw new ProviderConfigurationException("Missing required field token");
        }

    }

    public static String moduleName() {
        return ED_READY_READER;
    }

    public static CloseableHttpClient createHttpClient() {
        HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
        setTimeout(httpClientBuilder);
        return httpClientBuilder.build();
    }

    public static void setTimeout(HttpClientBuilder httpClientBuilder) {
        RequestConfig requestConfig = RequestConfig.custom().setConnectionRequestTimeout(120 * 1000).setConnectTimeout(120 * 1000).setSocketTimeout(900 * 1000).build();
        httpClientBuilder.setDefaultRequestConfig(requestConfig);
    }

    public static String fetchResponseBody(CloseableHttpResponse closeableHttpResponse, String responseBody) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        JsonGenerator jsonGenerator = new JsonFactory().createGenerator(byteArrayOutputStream, JsonEncoding.UTF8);
        try {
            jsonGenerator.writeStartObject();
            jsonGenerator.setCodec(new ObjectMapper());
            if (Objects.nonNull(closeableHttpResponse.getStatusLine())) {
                jsonGenerator.writeFieldName("status");
                jsonGenerator.writeStartObject();
                jsonGenerator.writeStringField("reason-phrase", closeableHttpResponse.getStatusLine().getReasonPhrase());
                jsonGenerator.writeNumberField("status-code", closeableHttpResponse.getStatusLine().getStatusCode());
                jsonGenerator.writeEndObject();
            }
            if (StringUtils.isNotEmpty(responseBody) && JsonUtil.verifyJson(responseBody)) {
                Map<String, Object> parsedResponseBody = JsonUtil.parseJson(responseBody);
                for (Map.Entry<String, Object> objectEntry : parsedResponseBody.entrySet()) {
                    jsonGenerator.writeFieldName(objectEntry.getKey());
                    jsonGenerator.writeObject(objectEntry.getValue());
                }
            }
            if (closeableHttpResponse.getAllHeaders().length > 0) {
                jsonGenerator.writeFieldName("headers");
                jsonGenerator.writeStartArray();
                for (Header header : closeableHttpResponse.getAllHeaders()) {
                    jsonGenerator.writeStartObject();
                    jsonGenerator.writeStringField(header.getName(), header.getValue());
                    jsonGenerator.writeEndObject();
                }
                jsonGenerator.writeEndArray();
            }
            jsonGenerator.writeEndObject();
            jsonGenerator.close();
            return byteArrayOutputStream.toString();
        } catch (IOException ioException) {
            log.error(ioException);
            throw new IOException("JSON generator encountered issues with IO: " + ioException.getMessage(), ioException);
        } finally {
            try {
                byteArrayOutputStream.close();
            } catch (IOException ignored) {
                log.error(ignored);
            }
        }
    }

    @Override
    protected Pair<List<JavaRDD<String>>, List<Dataset<Row>>> executeRDD(JavaSparkContext javaSparkContext, SQLContext sqlContext, Map<String, Object> map, Map<String, Object> context) throws DataSourceReadException {
        List<JavaRDD<String>> rdds = new ArrayList<>();
        List<Map<String, Object>> results = new ArrayList<>();

        try {
            this.token = getAuthToken();

            if (StringUtils.isNotBlank(this.pagination)) {
                JSONObject paginationJson = new JSONObject(this.pagination);
                int currentPage = 1, limit = 500, totalPages = 1;

                if (paginationJson.has("page")) {
                    currentPage = paginationJson.getInt("page");
                    totalPages = currentPage;
                }

                if (paginationJson.has("limit")) {
                    limit = paginationJson.getInt("limit");
                }

                do {
                    this.closeableHttpClient = createHttpClient();

                    List<Header> headers = new ArrayList<>();
                    headers.add(new BasicHeader(AUTHORIZATION, "Bearer" + this.token));

                    String questionMarkOrAmpersand = getSeparator(this.url);
                    String url = UrlEncodeHelper.getProperlyEncodedUrl(this.url + questionMarkOrAmpersand + "limit=" + limit + "&page=" + currentPage);

                    log.info("URL - " + url);

                    HttpResult httpResult = executeRequest(this.closeableHttpClient, headers, url, "");

                    if (httpResult.isSuccess()) {
                        String response = httpResult.getResponseBody();
                        Map<String, Object> edReadyResMap = JsonUtil.parseJson(response);

                        totalPages = Integer.parseInt(edReadyResMap.get("totalPages").toString());

                        if (edReadyResMap.containsKey("students")) {
                            Object students = edReadyResMap.get("students");
                            List<Map<String, Object>> studentsMap = JsonUtil.parseJsonList(new ObjectMapper().writeValueAsString(students));
                            results.addAll(studentsMap);
                        } else if (edReadyResMap.containsKey("goals")) {
                            Object goals = edReadyResMap.get("goals");
                            List<Map<String, Object>> goalsMap = JsonUtil.parseJsonList(new ObjectMapper().writeValueAsString(goals));
                            results.addAll(goalsMap);
                        } else {
                            List<Map<String, Object>> resultsMap = JsonUtil.parseJsonObjectOrList(response);
                            results.addAll(resultsMap);
                        }
                        currentPage++;
                    } else {
                        if (httpResult.getResponseCode() == 500) {
                            Map<String, Object> errorMap = JsonUtil.parseJson(httpResult.getResponseBody());
                            if (errorMap.containsKey("message")) {
                                String msg = errorMap.get("message").toString();
                                if (msg.equalsIgnoreCase("Token error")) {
                                    log.info("Token Expired. Regenerating token......");
                                    token = getAuthToken();
                                }
                            } else {
                                throw new DataSourceReadException("HTTP endpoint returned response: \n" + httpResult.getResponseBody());
                            }
                        } else {
                            throw new DataSourceReadException("HTTP endpoint returned response: \n" + httpResult.getResponseBody());
                        }
                    }
                } while (currentPage <= totalPages);
            } else {
                this.closeableHttpClient = createHttpClient();

                List<Header> headers = new ArrayList<>();
                headers.add(new BasicHeader(AUTHORIZATION, "Bearer" + token));

                String url = UrlEncodeHelper.getProperlyEncodedUrl(this.url);

                log.info("URL - " + url);

                HttpResult httpResult = executeRequest(this.closeableHttpClient, headers, url, "");
                String response = httpResult.getResponseBody();

                Map<String, Object> edReadyResMap = JsonUtil.parseJson(response);

                if (edReadyResMap.containsKey("students")) {
                    Object students = edReadyResMap.get("students");
                    List<Map<String, Object>> studentsMap = JsonUtil.parseJsonList(new ObjectMapper().writeValueAsString(students));
                    results.addAll(studentsMap);
                } else if (edReadyResMap.containsKey("goals")) {
                    Object goals = edReadyResMap.get("goals");
                    List<Map<String, Object>> goalsMap = JsonUtil.parseJsonList(new ObjectMapper().writeValueAsString(goals));
                    results.addAll(goalsMap);
                } else {
                    List<Map<String, Object>> resultsMap = JsonUtil.parseJsonObjectOrList(response);
                    resultsMap.addAll(resultsMap);
                }
            }
        } catch (IOException e) {
            throw new DataSourceReadException(e.getMessage());
        }

        addToSpark(javaSparkContext, context, rdds, results);

        if (!rdds.isEmpty()) {
            return Pair.with(rdds, Collections.emptyList());
        }

        List<JavaRDD<String>> list = new ArrayList<>(1);
        list.add(javaSparkContext.emptyRDD());
        return Pair.with(list, Collections.emptyList());
    }

    public String getSeparator(String url) {
        return url.indexOf('?') > -1 ? "&" : "?";
    }

    public String getAuthToken() throws DataSourceReadException, IOException {
        // response from auth api
        String res = fetchAuthToken();
        Map<String, Object> resMap = JsonUtil.parseJson(res);

        // return on token exists or else throw exception
        if (resMap.containsKey("token")) {
            return resMap.get("token").toString();
        } else {
            throw new DataSourceReadException("HTTP endpoint returned response: \n" + res);
        }
    }

    private void addToSpark(JavaSparkContext javaSparkContext, Map<String, Object> context, List<JavaRDD<String>> rdds, List<Map<String, Object>> mapList) throws DataSourceReadException {
        List<Map<String, Object>> resultsToAdd = new ArrayList<>(mapList.size());

        try {
            for (Map<String, Object> row : mapList) {
//                row.put("__parameters", context);
                resultsToAdd.add(row);
            }

            if (!resultsToAdd.isEmpty()) {
                JavaRDD<String> rdd = RDDUtil.jsonStringToRDD(javaSparkContext, JsonUtil.serializeJson(resultsToAdd));
                rdds.add(rdd);
            }
        } catch (IOException ioException) {
            log.error(ioException);
            throw new DataSourceReadException(ioException);
        }
    }


    public String fetchAuthToken() throws DataSourceReadException, IOException {
        CloseableHttpClient closeableHttpClient = createHttpClient();
        List<Header> headers = new ArrayList<>();
        headers.add(new BasicHeader(ACCEPT, "*/*"));
        headers.add(new BasicHeader(AUTHORIZATION, "Bearer" + superToken));

        String url = UrlEncodeHelper.getProperlyEncodedUrl(AUTH_URL);
        RequestBuilder requestBuilder = RequestBuilder.create(HttpMethods.POST).setUri(url);

        for (Header header : headers) {
            requestBuilder = requestBuilder.addHeader(header);
        }

        log.info("Executing request --> " + requestBuilder.getMethod() + " " + requestBuilder.getUri());

        HttpUriRequest httpUriRequest = requestBuilder.build();
        CloseableHttpResponse closeableHttpResponse = closeableHttpClient.execute(httpUriRequest);

        if (closeableHttpResponse.getStatusLine().getStatusCode() >= 400) {
            HttpEntity httpentity = closeableHttpResponse.getEntity();
            InputStream inputStream = httpentity.getContent();
            String responseBody = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
            String responseMessage = fetchResponseBody(closeableHttpResponse, responseBody);

            log.error("Authentication Failed : " + responseBody);

            throw new DataSourceReadException("HTTP endpoint returned response: \n" + responseMessage);
        }

        HttpResult httpResult = getHttpResult(closeableHttpResponse);
        log.info("execution response code --> " + httpResult.getResponseCode());

        return httpResult.getResponseBody();
    }

    private HttpResult executeRequest(CloseableHttpClient closeableHttpClient, List<Header> headers, String url, String requestBody) throws DataSourceReadException {
        String responseBody = "";

        if (StringUtils.isEmpty(requestBody)) {
            requestBody = "";
        }

        try {
            RequestBuilder requestBuilder = RequestBuilder.create(this.httpMethod).setUri(url);

            for (Header header : headers) {
                requestBuilder = requestBuilder.addHeader(header);
            }

            if (StringUtils.isNotEmpty(requestBody)) {
                StringEntity stringEntity = new StringEntity(requestBody, StandardCharsets.UTF_8);
                stringEntity.setContentType(new BasicHeader(HTTP.CONTENT_TYPE, "application/json"));
                requestBuilder = requestBuilder.setEntity(stringEntity);
            }

            log.info("Executing request --> " + requestBuilder.getMethod() + " " + requestBuilder.getUri());

            HttpUriRequest httpUriRequest = requestBuilder.build();
            CloseableHttpResponse closeableHttpResponse = closeableHttpClient.execute(httpUriRequest);

            if (closeableHttpResponse.getStatusLine().getStatusCode() >= 400) {
                HttpEntity httpentity = closeableHttpResponse.getEntity();
                InputStream inputStream = httpentity.getContent();
                responseBody = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
                String responseMessage = fetchResponseBody(closeableHttpResponse, responseBody);

                // handling 401 request for re-execution
                HttpResult httpResult = new HttpResult();

                httpResult.setSuccess(false);
                httpResult.setResponseCode(closeableHttpResponse.getStatusLine().getStatusCode());
                httpResult.setResponseBody(responseMessage);

                log.error("Authentication Failed : " + responseMessage);
                return httpResult;
            }

            HttpResult httpResult = getHttpResult(closeableHttpResponse);
            log.info("execution response code --> " + httpResult.getResponseCode());
            return httpResult;
        } catch (IOException e) {
            throw new DataSourceReadException("Error while executing the request : " + e);
        }
    }

    private HttpResult getHttpResult(CloseableHttpResponse closeableHttpResponse) throws IOException {
        HttpResult httpResult = new HttpResult();

        HttpEntity httpEntity = closeableHttpResponse.getEntity();
        InputStream inputStream = httpEntity.getContent();
        String responseBody = IOUtils.toString(inputStream, StandardCharsets.UTF_8);

        EntityUtils.consume(httpEntity);
        if (closeableHttpResponse.getStatusLine().getStatusCode() >= HttpStatusCodes.STATUS_CODE_BAD_REQUEST) {
            httpResult.setSuccess(false);
            httpResult.setResponseCode(closeableHttpResponse.getStatusLine().getStatusCode());
            httpResult.setResponseBody(responseBody);
        } else {
            httpResult.setSuccess(true);
            httpResult.setResponseCode(closeableHttpResponse.getStatusLine().getStatusCode());
            httpResult.setResponseBody(responseBody);
        }
        return httpResult;
    }
}
