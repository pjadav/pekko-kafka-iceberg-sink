package io.github.prakashjadav.icebergsink.components

import io.github.prakashjadav.icebergsink.TestFixtures

class IcebergFlowSpec extends TestFixtures {

  "IcebergCatalog" should "honour an explicit warehouse override" in {
    IcebergCatalog.warehouse(
      appConfig.copy(
        icebergWarehouse = Some("s3://example-bucket/warehouse/")
      )
    ) shouldBe "s3://example-bucket/warehouse/"
  }

  it should "include optional Glue client properties when configured" in {
    val properties = IcebergCatalog.catalogProperties(
      appConfig.copy(
        icebergGlueCatalogId         = Some("123456789012"),
        icebergCatalogClientFactory  = Some("org.apache.iceberg.aws.AssumeRoleAwsClientFactory"),
        icebergAssumeRoleArn         = Some("arn:aws:iam::123456789012:role/example-iceberg-writer"),
        icebergAssumeRoleRegion      = Some("us-east-1"),
        icebergAssumeRoleSessionName = Some("example-iceberg-writer")
      )
    )

    properties.get("glue.id") shouldBe "123456789012"
    properties.get("client.factory") shouldBe "org.apache.iceberg.aws.AssumeRoleAwsClientFactory"
    properties.get(
      "client.assume-role.arn"
    ) shouldBe "arn:aws:iam::123456789012:role/example-iceberg-writer"
    properties.get("client.assume-role.region") shouldBe "us-east-1"
    properties.get("client.assume-role.session-name") shouldBe "example-iceberg-writer"
  }
}
