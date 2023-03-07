package io.lingk.transformer.connectors;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.http.HttpMethods;
import com.google.api.client.http.HttpStatusCodes;
import io.lingk.transformer.http.HttpResult;
import io.lingk.transformer.json.JsonUtil;
import io.lingk.transformer.provider.exception.DataSourceReadException;
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.lingk.transformer.Constants.ACCEPT;
import static org.apache.http.HttpHeaders.AUTHORIZATION;

@Log4j2
public class Temp {

    private static final String AUTH_URL = "https://tx.apiedready.edready.org/api/v2/tokens/admin";
    private static final String superToken = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE2OTI4ODQwNDUsImVtYWlsIjoidGNiYWRtaW4wMDFAZWRyZWFkeS5vcmciLCJpYXQiOjE2NzcyNDU2NDV9.BExNqZGbAsBJ9PrAP1N_kaNJ_o1uSRcSmPGlip3qZSk";
    private static final boolean isContinue = true;
    private static String token = "";

    public static void main(String[] args) throws DataSourceReadException, IOException {
        Temp t = new Temp();
        token = t.getAuthToken();
        System.out.println("Auth token - " + token);

        List<Map<String, Object>> map = new ArrayList<>();

        int currentPage = 1;
        int limit = 500;
        int totalPages = 1;
        int studentsTotal = 0;

        do {
            CloseableHttpClient httpClient = createHttpClient();

            List<Header> headers = new ArrayList<>();
            headers.add(new BasicHeader(AUTHORIZATION, "Bearer" + token));
            String questionMarkOrAmpersand = t.getSeparator("https://tx.apiedready.edready.org/api/v2/studentsByCustomer?hours=26");
            String url = UrlEncodeHelper.getProperlyEncodedUrl("https://tx.apiedready.edready.org/api/v2/studentsByCustomer?hours=26" + questionMarkOrAmpersand + "limit=" + limit + "&page=" + currentPage);

            System.out.println("separator : " + questionMarkOrAmpersand);

            System.out.println("URL - " + url);

            HttpResult httpResult = t.executeRequest(httpClient, headers, url, "");

            if (httpResult.isSuccess()) {
                String response = httpResult.getResponseBody();
                Map<String, Object> edReadyResMap = JsonUtil.parseJson(response);

                totalPages = Integer.parseInt(edReadyResMap.get("totalPages").toString());
                studentsTotal = Integer.parseInt(edReadyResMap.get("studentsTotal").toString());

                if (edReadyResMap.containsKey("students")) {
                    Object students = edReadyResMap.get("students");
                    List<Map<String, Object>> studentsMap = JsonUtil.parseJsonList(new ObjectMapper().writeValueAsString(students));
                    map.addAll(studentsMap);
                } else if (edReadyResMap.containsKey("goals")) {
                    Object goals = edReadyResMap.get("goals");
                    List<Map<String, Object>> goalsMap = JsonUtil.parseJsonList(new ObjectMapper().writeValueAsString(goals));
                    map.addAll(goalsMap);
                } else {
                    List<Map<String, Object>> results = JsonUtil.parseJsonObjectOrList(response);
                    map.addAll(results);
                }
//            studentsMap = JsonUtil.parseJsonList(new ObjectMapper().writeValueAsString(students));
                currentPage++;
//            map.addAll(studentsMap);
            } else {
                if (httpResult.getResponseCode() == 500) {
                    Map<String, Object> errorMap = JsonUtil.parseJson(httpResult.getResponseBody());
                    if (errorMap.containsKey("message")) {
                        String msg = errorMap.get("message").toString();
                        if (msg.equalsIgnoreCase("Token error")) {
                            System.out.println("Token Expired. Regenerating token......");
                            token = t.getAuthToken();
                        }
                    } else {
                        throw new DataSourceReadException("HTTP endpoint returned response: \n" + httpResult.getResponseBody());
                    }
                } else {
                    throw new DataSourceReadException("HTTP endpoint returned response: \n" + httpResult.getResponseBody());
                }
            }
        } while (currentPage <= totalPages);

        System.out.println(studentsTotal == map.size());
//        t.executeRequest()
    }

//    public static void main(String[] args) throws IOException, DataSourceReadException {
//        String res = "{\"status\":{\"reason-phrase\":\"\",\"status-code\":500},\"exception\":\"JWT is expired: eyJhbGciOiJIUzI1NiJ9.eyJwYXJ0bmVyIjpmYWxzZSwiY3VzdG9tZXJJZCI6MzczNjkxLCJ1c2VyVHlwZSI6ImFkbWluIiwiZXhwIjoxNjc4MTI1NTkzLCJ1c2VySWQiOjE1MTAzNzEsImlhdCI6MTY3ODEyNTI5MywidXNlcm5hbWUiOiJ0Y2JhZG1pbjAwMUBlZHJlYWR5Lm9yZyJ9.lVd1zFQMyqcAEVBjDplYi9NKlQ2k7t1uAM-rx0RPWCc\",\"message\":\"Token error\",\"headers\":[{\"Date\":\"Mon, 06 Mar 2023 17:59:59 GMT\"},{\"Content-Type\":\"application/json;charset=UTF-8\"},{\"Connection\":\"keep-alive\"},{\"Set-Cookie\":\"locale=en; Path=/; SameSite=none; Secure; HttpOnly;\"},{\"Vary\":\"Origin\"},{\"Vary\":\"Access-Control-Request-Method\"},{\"Vary\":\"Access-Control-Request-Headers\"},{\"X-XSS-Protection\":\"1; mode=block\"},{\"Cache-Control\":\"no-cache, no-store, max-age=0, must-revalidate\"},{\"Pragma\":\"no-cache\"},{\"Expires\":\"0\"}]}";
//        Map<String, Object> errorMap = JsonUtil.parseJson(res);
//        if (errorMap.containsKey("message")) {
//            String msg = errorMap.get("message").toString();
//            if (msg.equalsIgnoreCase("Token error")) {
//                log.info("Token Expired. Regenerating token......");
////                token = t.getAuthToken();
//            }
//        } else {
//            throw new DataSourceReadException("HTTP endpoint returned response: \n" + res);
//        }
//    }

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

    public String fetchAuthToken() throws DataSourceReadException, IOException {
        CloseableHttpClient closeableHttpClient = createHttpClient();
        List<Header> headers = new ArrayList<>();
        headers.add(new BasicHeader(ACCEPT, "*/*"));
        headers.add(new BasicHeader(AUTHORIZATION, superToken));

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
            RequestBuilder requestBuilder = RequestBuilder.create(HttpMethods.GET).setUri(url);

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
