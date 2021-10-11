package com.github.gabrielfcarrijo.gcp.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import javax.xml.transform.sax.SAXResult;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Main {

  private static final String PROJECT_ID = "capable-memory-328313";
  private static final String BUCKET_NAME = "order-management_reverse-tracking_correios-contracts";
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String OBJECT_NAME_PATTERN = "%s_%s.json";

  public static void main(String[] args) throws IOException {

//    final var contract = Contract.builder()
//        .tenantId("TenantA")
//        .administrativeCode("123")
//        .cardNumber("2")
//        .logisticsUsername("User-Logistic")
//        .reverseUsername("User-Reverse")
//        .build();
//
//    create(contract);

//    var contract = download("TenantA", "123");
//
//    log.info("{}", contract);
    delete("TenantA", "123");
   }

  private static void create(Contract contract) throws IOException {
    Storage storage = StorageOptions.newBuilder().setProjectId(PROJECT_ID).build().getService();
    BlobId blobId = BlobId.of(BUCKET_NAME, resolveObjectName(contract));
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
    storage.create(blobInfo, MAPPER.writeValueAsBytes(contract));
  }

  private static Contract download(String tenant, String administrativeCode) throws IOException {
    Storage storage = StorageOptions.newBuilder().setProjectId(PROJECT_ID).build().getService();
    BlobId blobId = BlobId
        .of(BUCKET_NAME, String.format(OBJECT_NAME_PATTERN, tenant, administrativeCode));
    var blob = storage.get(blobId);
    return MAPPER.readValue(blob.getContent(), Contract.class);
  }

  private static void delete(String tenant, String administrativeCode) throws IOException {
    Storage storage = StorageOptions.newBuilder().setProjectId(PROJECT_ID).build().getService();
    BlobId blobId = BlobId.of(BUCKET_NAME, String.format(OBJECT_NAME_PATTERN, tenant, administrativeCode));
    storage.delete(blobId);
  }

  private static String resolveObjectName(Contract contract) {
    return String
        .format(OBJECT_NAME_PATTERN, contract.getTenantId(), contract.getAdministrativeCode());
  }

  @NoArgsConstructor
  @AllArgsConstructor
  @Getter
  @Builder
  @ToString
  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
  private static class Contract {

    private String tenantId;
    private String administrativeCode;
    private String cardNumber;
    private String logisticsUsername;
    private String reverseUsername;
  }
}
