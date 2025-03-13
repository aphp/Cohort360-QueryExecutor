package fr.aphp.id.eds.requester.cohort.fhir

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{BooleanType, LongType, StructField, StructType}
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import fr.aphp.id.eds.requester.{FhirServerConfig, ResultColumn}
import fr.aphp.id.eds.requester.query.resolver.rest.DefaultRestFhirClient
import org.apache.spark.sql.SparkSession
import org.hl7.fhir.r4.model.ListResource.ListMode
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

class FhirCohortCreationTest extends AnyFunSuiteLike with Matchers with BeforeAndAfterEach {
  val Port = 8080
  val Host = "localhost"
  val wireMockServer = new WireMockServer(wireMockConfig().port(Port))
  val fhirCohortCreationService = new FhirCohortCreation(
    new DefaultRestFhirClient(FhirServerConfig("http://" + Host + ":" + Port, None, None),
                              cohortServer = true)
  )
  // For debugging
  wireMockServer.addMockServiceRequestListener((request, response) => {
    println(request)
    println(response)
  })

  override def beforeEach: Unit = {
    wireMockServer.start()
    WireMock.configureFor(Host, Port)
  }

  override def afterEach: Unit = {
    wireMockServer.stop()
  }

  test("testCreateCohort") {
    wireMockServer.addStubMapping(
      WireMock
        .post(WireMock.urlEqualTo("/List"))
        .willReturn(
          WireMock
            .aResponse()
            .withStatus(201)
            .withHeader("Content-Type", "application/json")
            .withBody(
              """
              {
                "resourceType": "List",
                "id": "1"
              }
              """
            )
        )
        .build()
    )
    wireMockServer.addStubMapping(
      WireMock
        .get(WireMock.urlEqualTo("/metadata"))
        .willReturn(
          WireMock
            .aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(
              """
                {
                    "resourceType": "CapabilityStatement",
                    "id": "1"
                }
              """
            )
        )
        .build()
    )
    fhirCohortCreationService.createCohort("test",
                                           Some("test"),
                                           "test",
                                           "test",
                                           "test",
                                           None,
                                           ListMode.SNAPSHOT,
                                           1) should be(1)
    wireMockServer.verify(1, WireMock.postRequestedFor(WireMock.urlEqualTo("/List")))
    wireMockServer.verify(1, WireMock.getRequestedFor(WireMock.urlEqualTo("/metadata")))
    wireMockServer.listAllStubMappings().getMappings.forEach { mapping =>
      wireMockServer.verify(WireMock.exactly(1), WireMock.requestMadeFor(mapping.getRequest))
    }
  }

  test("testUpdateCohort") {
    wireMockServer.addStubMapping(
      WireMock
        .get(WireMock.urlEqualTo("/List/1"))
        .willReturn(
          WireMock
            .aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(
              """
              {
                "resourceType": "List",
                "id": "1"
              }
              """
            )
        )
        .build()
    )
    wireMockServer.addStubMapping(
      WireMock
        .put(WireMock.urlEqualTo("/List/1"))
        .withRequestBody(
          WireMock.equalToJson(
            """
{
    "entry": [
        {
            "deleted": false,
            "item": {
                "id": "1",
                "reference": "Patient/1"
            }
        },
        {
            "deleted": false,
            "item": {
                "id": "2",
                "reference": "Patient/2"
            }
        },
        {
            "deleted": false,
            "item": {
                "id": "3",
                "reference": "Patient/3"
            }
        },
        {
            "deleted": false,
            "item": {
                "id": "4",
                "reference": "Patient/4"
            }
        },
        {
            "deleted": false,
            "item": {
                "id": "5",
                "reference": "Patient/5"
            }
        },
        {
            "deleted": false,
            "item": {
                "id": "6",
                "reference": "Patient/6"
            }
        },
        {
            "deleted": false,
            "item": {
                "id": "7",
                "reference": "Patient/7"
            }
        },
        {
            "deleted": false,
            "item": {
                "id": "8",
                "reference": "Patient/8"
            }
        },
        {
            "deleted": false,
            "item": {
                "id": "9",
                "reference": "Patient/9"
            }
        },
        {
            "deleted": true,
            "item": {
                "id": "10",
                "reference": "Patient/10"
            }
        },
        {
            "deleted": false,
            "item": {
                "id": "11",
                "reference": "Patient/11"
            }
        },
        {
            "deleted": false,
            "item": {
                "id": "12",
                "reference": "Patient/12"
            }
        }
    ],
    "id": "1",
    "resourceType": "List"
}
                        """,
            true,
            false
          )
        )
        .willReturn(
          WireMock
            .aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(
              """
              {
                "resourceType": "List",
                "id": "1"
              }
              """
            )
        )
        .build()
    )
    wireMockServer.addStubMapping(
      WireMock
        .get(WireMock.urlEqualTo("/metadata"))
        .willReturn(
          WireMock
            .aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(
              """
                {
                    "resourceType": "CapabilityStatement",
                    "id": "1"
                }
              """
            )
        )
        .build()
    )

    implicit val sparkSession: SparkSession =
      SparkSession.builder().master("local[*]").getOrCreate()

    val data = Seq(
      Row(1L, false),
      Row(2L, false),
      Row(3L, false),
      Row(4L, false),
      Row(5L, false),
      Row(6L, false),
      Row(7L, false),
      Row(8L, false),
      Row(9L, false),
      Row(10L, true),
      Row(11L, false),
      Row(12L, false)
    )
    val schema = StructType(
      Seq(
        StructField(ResultColumn.SUBJECT, LongType, nullable = false),
        StructField("deleted", BooleanType, nullable = false)
      ))
    val df: DataFrame = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(data),
      schema
    )

    fhirCohortCreationService.updateCohort(1, df, null, 12, delayCohortCreation = false, "Patient")

    wireMockServer.verify(1, WireMock.putRequestedFor(WireMock.urlEqualTo("/List/1")))
    wireMockServer.verify(1, WireMock.getRequestedFor(WireMock.urlEqualTo("/metadata")))

  }

  test("testReadCohortEntries") {
    implicit val sparkSession: SparkSession =
      SparkSession.builder().master("local[*]").getOrCreate()
    wireMockServer.addStubMapping(
      WireMock
        .get(WireMock.urlEqualTo("/List/1"))
        .willReturn(
          WireMock
            .aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(
              """
                  {
                    "resourceType": "List",
                    "id": "1",
                    "entry": [
                      {
                        "item": {
                          "reference": "Patient/1"
                        }
                      }
                    ]
                  }
                  """
            )
        )
        .build()
    )
    wireMockServer.addStubMapping(
      WireMock
        .get(WireMock.urlPathEqualTo("/List"))
        .withQueryParam("identifier", WireMock.equalTo("1"))
        .willReturn(
          WireMock
            .aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(
              """
                  {
                    "resourceType": "Bundle",
                    "id": "bundle-id",
                    "type": "searchset",
                    "entry": [
                      {
                        "resource": {
                          "resourceType": "List",
                          "mode": "snapshot",
                          "id": "2",
                          "entry": [
                            {
                              "item": {
                                "reference": "Patient/4"
                              }
                            }
                          ]
                        }
                      },{
                        "resource": {
                          "resourceType": "List",
                          "mode": "changes",
                          "id": "2",
                          "entry": [
                            {
                              "item": {
                                "reference": "Patient/1"
                              },
                              "deleted": true
                            },
                            {
                              "item": {
                                "reference": "Patient/2"
                              }
                            }
                          ]
                        }
                      },
                     {
                        "resource": {
                          "resourceType": "List",
                          "mode": "changes",
                          "id": "2",
                          "entry": [
                            {
                              "item": {
                                "reference": "Patient/3"
                              }
                            },
                            {
                              "item": {
                                "reference": "Patient/2"
                              },
                              "deleted": true
                            }
                          ]
                        }
                      },
                     {
                        "resource": {
                          "resourceType": "List",
                          "mode": "changes",
                          "id": "2",
                          "entry": [
                            {
                              "item": {
                                "reference": "Patient/1"
                              }
                            }
                          ]
                        }
                      }
                    ]
                  }
                  """
            )
        )
        .build()
    )
    wireMockServer.addStubMapping(
      WireMock
        .get(WireMock.urlEqualTo("/metadata"))
        .willReturn(
          WireMock
            .aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(
              """
                    {
                        "resourceType": "CapabilityStatement",
                        "id": "1"
                    }
                  """
            )
        )
        .build()
    )

    val df = fhirCohortCreationService.readCohortEntries(1)
    assert(df.count() == 2)
    assert(df.collect()(0).getAs[Long]("_itemreferenceid") == 3)
    assert(df.collect()(1).getAs[Long]("_itemreferenceid") == 1)
  }

}
