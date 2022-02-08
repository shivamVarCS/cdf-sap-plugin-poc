package com.sap.delta.watcher;

import com.google.api.gax.paging.Page;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class GcsWatcher {

  public static void main(String[] args) throws IOException {
    String cred = "{\n" +
      "  \"type\": \"service_account\",\n" +
      "  \"project_id\": \"sap-adapter\",\n" +
      "  \"private_key_id\": \"04bfd02934e2ec881719d66be18678f375d2c3db\",\n" +
      "  \"private_key\": \"-----BEGIN PRIVATE KEY-----\\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC1G5i+BTSw3h9t\\n61XWOSMa7mdIC0Gbec3HIA0qLGdTVTqXkW1BRKEBtJB+hlVrVLCnopBopi7BbjkR\\nvSrK3bSb5fjkUR2XxP2kDh+nSLN29n/gCZcO12xGsEBtn5d/dFy8nHEQ2opbM16R\\nxcpIi7LMedp9Mo+Fja0Lj9NotM5soH9IpfNgUzYWHcBQDkN+nSmZ7wkDvAR2UdrR\\nnYJSLRYq4ugRkJZJqKsoqYkhioQd9PuqC3J5tP3eN9t4DSs+jxkkxz3yBfy/lR2d\\nKPeRGW+quEhUkLanaFLW+VgVB2qtSCc2IsA+vhGGldKGfEHR21hbCyu5r9dP/Ajn\\nMCtLe803AgMBAAECggEAJxiHfxG5n6q7pytj0sRv1xr68br4VTtUmDVLkjyWq48d\\nAGY06x+JEEit3ppaKzrTjcSSnvys4DXOnQ00hSmaGQNqmMMH++DbDkS8QYz4rOgm\\nj7MSSHJpndLjfaazeiCg+lhdMhGzR1/N/PLxKXr6xJhTea5l/CMq5XGNZi+N1jXs\\nelOOa1/1mEHomkNo0z4udAYsj8NeBeqg/PBanCO00IE9uFZBsZfE2iNwaoD0izA9\\npTj+nVcUFfpn5tfgu1s0TPD0jl1MnIhCPQyBL0ainpdQKn7WL+uCQwAdj+DSeFxq\\nkxHLuxer7mJiSnNqkNtPUvfAzVK5OzeP6ldkSMxpeQKBgQDavAG+OVP+3hfPVpGm\\nbpscFy6QWOTWwjj4t2xmzBpC8URLsNQtCKli8BJNPmTnvVNqncWv94dJnZZ0+Zo9\\neWnbopsGuFayUaF+qLCIOmZWq4qjKpXgDuB1xdAiM6+n+rNyj6zor6o4TZt46/XX\\nzNM7KnHCo5os8KMp4lGJR75VLwKBgQDT9oYsph3zYrNLa25jopP1dWwqNVB5QRv8\\n/Yg3X0WguhpEu6VidkBs5HPUd0jXUc5dK4d5mPBKcMXGDKvPLRgAOhYc1kFDnFc4\\nZ5Y+CJhyDC5KvMII11gdzU6DJ8VbJ37EPUj6mh4GzZP3BlBKnqk9AUd9Pifb75iC\\nVnteviSWeQKBgD3hxKh5dY78bEYHWst4LT/EOzMxQwSTDCUpV2y5v5BCy6ySSWS4\\nN5u7CaJ6zXdWc7wNCLmg0NB92ewcFYvgxpcRqMQ88KIgQA0iUlcoFn3cqCtDSAWM\\nj+oh3aFfeAmQ/qWhiiyGTiUBp8ONFg9IblYlyuti96JRLggSdDjfaDSXAoGBAKim\\nYTHLZs9NvB0JOMccB5/pMwwOaZ5g2NUeITD1pB/zk40WxMuoDTDXQPZPhuyT0Z++\\nT8fHkYGZYzNmx5lLZupfTVLagwk5rwhZG2j2S92KpZafw79nIFxuC8c9cMFLloir\\nvpJu5+8/yThmlKIaxcudZfVX7D0J0Y37I6RJpDD5AoGBALK8nvH2DltG5ibNshgf\\nQHT0k1VkR3TA+iZseA6yuw48akmqWyTeKtzxXP2C2ViKadEq3+hQfDEUqdXQb4k9\\nWwUrjjf/etvQ9dAjXJTuX89H0r7z9A2GdcrfmXaNNyxBbalyPfnXe+m2aqe+PcNo\\n5Y2sg5Epq1hc5vgZ69Y7/GYX\\n-----END PRIVATE KEY-----\\n\",\n" +
      "  \"client_email\": \"sa-gcs-connectivity@sap-adapter.iam.gserviceaccount.com\",\n" +
      "  \"client_id\": \"106920579289590450795\",\n" +
      "  \"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",\n" +
      "  \"token_uri\": \"https://oauth2.googleapis.com/token\",\n" +
      "  \"auth_provider_x509_cert_url\": \"https://www.googleapis.com/oauth2/v1/certs\",\n" +
      "  \"client_x509_cert_url\": \"https://www.googleapis.com/robot/v1/metadata/x509/sa-gcs-connectivity%40sap-adapter.iam.gserviceaccount.com\"\n" +
      "}";

    InputStream stream = new ByteArrayInputStream(cred.getBytes());
    Credentials credentials = GoogleCredentials.fromStream(stream);
    Storage storage =
      StorageOptions.newBuilder().setCredentials(credentials).setProjectId("sap-adapter").build().getService();

    Bucket bucket = storage.get("cdf-sap-dependent-files");

    ExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

    executorService.submit(() -> {
      long lastReadTime = 0l;
      while (true) {
        Page<Blob> blobPage = bucket.list(Storage.BlobListOption.prefix("x509-cert/test " +
          "part/more_part$test/"));
        Iterator<Blob> iterator = blobPage.getValues().iterator();
        while (iterator.hasNext()) {
          Blob blobx = iterator.next();
          if (blobx.getName().endsWith("/")) {
            continue;
          }
          if (blobx.getCreateTime() > lastReadTime) {
            System.out.println("Id: " + blobx.getBlobId());
            System.out.println("Name: " + blobx.getName());
            System.out.println("File size: " + blobx.getSize());
            System.out.println("Created time: " + blobx.getCreateTime());
            System.out.println("Encoding: " + blobx.getContentEncoding());
            System.out.println("Language: " + blobx.getContentLanguage());
            System.out.println("Type: " + blobx.getContentType());
            System.out.println("getMetageneration: " + blobx.getMetageneration());
            System.out.println("Content: " + new String(blobx.getContent(), StandardCharsets.UTF_8));
            System.out.println();
            lastReadTime = blobx.getCreateTime();
          }
        }
        TimeUnit.SECONDS.sleep(2);
      }
    });
  }
}
