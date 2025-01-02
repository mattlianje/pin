package pin

class PinTest extends munit.FunSuite {
  import core._

  override def beforeEach(context: BeforeEach): Unit = {
    core.clear()
  }

  test("pin@ decoration and dot generation") {
    val pipeline = `@pin`("etl") {
      object Pipeline {
        val steps = List("extract", "transform", "load")
      }
      Pipeline
    }

    val basicPipeline = `@pin`("basic-etl", upstream = Set("etl")) {
      object BasicPipeline {
        val steps = List("extract", "load")
      }
      BasicPipeline
    }

    val advancedPipeline =
      `@pin`("advanced-etl", upstream = Set("basic-etl", "etl")) {
        object AdvancedPipeline {
          val steps = List("extract", "transform", "load", "validate")
        }
        AdvancedPipeline
      }

    val expected = """digraph G {
  node [shape=box];
  edge [dir=forward];
  advanced_etl;
  basic_etl;
  etl;

  basic_etl -> advanced_etl;
  etl -> advanced_etl;
  etl -> basic_etl;
}"""

    assertEquals(dot.trim, expected.trim)
  }

  test("nested subgraphs") {
    `@subgraph`(
      "pipeline",
      label = Some("Data Pipeline"),
      dotOptions = Map("style" -> "filled", "color" -> "lightgrey")
    )

    `@subgraph`(
      "trading",
      label = Some("Trading Pipeline"),
      dotOptions = Map("color" -> "blue"),
      parent = Some("pipeline")
    )

    `@subgraph`(
      "market",
      label = Some("Market Data"),
      dotOptions = Map("color" -> "green"),
      parent = Some("trading")
    )

    val tradesRaw = `@pin`(
      "trades_raw",
      dotOptions = Map("shape" -> "cylinder"),
      subgraph = Some("trading")
    ) {
      object TradesRaw
    }

    val marketData = `@pin`(
      "market_data",
      dotOptions = Map("shape" -> "cylinder"),
      subgraph = Some("market"),
      upstream = Set("trades_raw")
    ) {
      object MarketData
    }

    val expected = """digraph G {
  node [shape=box];
  edge [dir=forward];

  subgraph cluster_pipeline {
    label = "Data Pipeline";
    style="filled";
    color="lightgrey";

    subgraph cluster_trading {
      label = "Trading Pipeline";
      color="blue";

      subgraph cluster_market {
        label = "Market Data";
        color="green";
        market_data[shape=cylinder];
      }
      trades_raw[shape=cylinder];
    }
  }

  trades_raw -> market_data;
}"""

    assertEquals(dot.trim, expected.trim)
  }

  test("pipeline visualization with descriptions and schedules") {
    core.clear()

    `@subgraph`(
      name = "pipeline",
      label = Some("Data Pipeline"),
      dotOptions = Map("style" -> "filled", "color" -> "lightgrey")
    )

    `@pin`(
      "ingest",
      description = Some("Data ingestion from S3"),
      schedule = Some("every 10m"),
      dotOptions = Map("shape" -> "cylinder"),
      subgraph = Some("pipeline")
    ) { "ingest" }

    `@pin`(
      "transform",
      description = Some("Clean and enrich data"),
      schedule = Some("every 15m"),
      upstream = Set("ingest"),
      subgraph = Some("pipeline")
    ) { "transform" }

    `@pin`(
      "load",
      description = Some("Load to warehouse"),
      schedule = Some("hourly"),
      upstream = Set("transform"),
      dotOptions = Map(
        "shape" -> "cylinder",
        "style" -> "filled",
        "fillcolor" -> "peachpuff"
      ),
      subgraph = Some("pipeline")
    ) { "load" }

    println(core.dot)
  }

  test("real world pipeline") {
    sealed trait Workflow
    sealed trait DataModel

    val CYLINDER_STYLE = Map("shape" -> "cylinder")
    val BOX_STYLE = Map("style" -> "filled", "fillcolor" -> "lightskyblue2")

    `@subgraph`(
      "trading",
      label = Some("Trading Data Pipeline"),
      dotOptions = Map("style" -> "filled", "color" -> "grey85")
    )
    `@subgraph`(
      "market",
      label = Some("Market Data Pipeline"),
      dotOptions = Map("style" -> "filled", "color" -> "oldlace")
    )
    `@subgraph`("reference", label = Some("Reference Data Pipeline"))
    `@subgraph`(
      "analytics",
      label = Some("Analytics Pipeline"),
      dotOptions = Map("color" -> "purple2")
    )

    `@pin`(
      "trades_raw",
      description = Some("S3"),
      dotOptions = CYLINDER_STYLE,
      subgraph = Some("trading")
    ) {}
    case class TradeData(
        timestamp: Long,
        symbol: String,
        price: Double,
        quantity: Int
    ) extends DataModel

    `@pin`(
      "trades_processed",
      description = Some("S3"),
      dotOptions = CYLINDER_STYLE,
      subgraph = Some("trading"),
      upstream = Set("process_trades")
    ) {}
    case class ProcessedTradeData(data: TradeData)

    `@pin`(
      "trades_aggregated",
      description = Some("Postgres"),
      dotOptions = CYLINDER_STYLE,
      subgraph = Some("trading"),
      upstream = Set("aggregate_trades")
    ) {}
    case class AggregatedTradeData(data: TradeData)

    `@pin`(
      "market_feed",
      description = Some("Kafka"),
      dotOptions = CYLINDER_STYLE,
      subgraph = Some("market")
    ) {}
    case class MarketData(
        symbol: String,
        bid: Double,
        ask: Double,
        timestamp: Long
    ) extends DataModel

    `@pin`(
      "market_processed",
      description = Some("S3"),
      dotOptions = CYLINDER_STYLE,
      subgraph = Some("market"),
      upstream = Set("ingest_market")
    ) {}
    case class ProcessedMarketData(data: MarketData)

    `@pin`(
      "market_analytics",
      description = Some("Redshift"),
      dotOptions = CYLINDER_STYLE,
      subgraph = Some("market"),
      upstream = Set("analyze_market")
    ) {}
    case class MarketAnalytics(data: MarketData)

    `@pin`(
      "ref_api",
      description = Some("REST"),
      dotOptions = CYLINDER_STYLE,
      subgraph = Some("reference")
    ) {}
    case class ReferenceData(symbol: String, cusip: String, sedol: String)
        extends DataModel

    `@pin`(
      "ref_raw",
      description = Some("S3"),
      dotOptions = CYLINDER_STYLE,
      subgraph = Some("reference"),
      upstream = Set("fetch_ref")
    ) {}
    case class RawReferenceData(data: ReferenceData)

    `@pin`(
      "ref_processed",
      description = Some("Postgres"),
      dotOptions = CYLINDER_STYLE,
      subgraph = Some("reference"),
      upstream = Set("process_ref")
    ) {}
    case class ProcessedReferenceData(data: ReferenceData)

    `@pin`(
      "analytics_raw",
      description = Some("Kafka"),
      dotOptions = CYLINDER_STYLE,
      subgraph = Some("analytics")
    ) {}
    case class AnalyticsData(symbol: String, metrics: Map[String, Double])
        extends DataModel

    `@pin`(
      "analytics_processed",
      description = Some("Snowflake"),
      dotOptions = CYLINDER_STYLE,
      subgraph = Some("analytics"),
      upstream = Set("process_analytics")
    ) {}
    case class ProcessedAnalyticsData(data: AnalyticsData)

    /* Processing */
    `@pin`(
      "process_trades",
      schedule = Some("every 11m"),
      upstream = Set("trades_raw", "ref_processed"),
      subgraph = Some("trading"),
      dotOptions = BOX_STYLE
    ) {}
    def processTrades(
        raw: TradeData,
        ref: ProcessedReferenceData
    ): ProcessedTradeData = ???

    `@pin`(
      "aggregate_trades",
      schedule = Some("hourly"),
      upstream = Set("trades_processed"),
      subgraph = Some("trading"),
      dotOptions = BOX_STYLE
    ) {}
    def aggregateTrades(data: ProcessedTradeData): AggregatedTradeData = ???

    `@pin`(
      "ingest_market",
      schedule = Some("every 1m"),
      upstream = Set("market_feed"),
      subgraph = Some("market"),
      dotOptions = BOX_STYLE
    ) {}
    def ingestMarket(feed: MarketData): ProcessedMarketData = ???

    `@pin`(
      "analyze_market",
      schedule = Some("daily @ 00:00"),
      upstream = Set("market_processed", "trades_processed", "ref_processed"),
      subgraph = Some("market"),
      dotOptions = BOX_STYLE
    ) {}
    def analyzeMarket(data: ProcessedMarketData): MarketAnalytics = ???

    `@pin`(
      "fetch_ref",
      schedule = Some("daily @ 08:00"),
      upstream = Set("ref_api"),
      subgraph = Some("reference"),
      dotOptions = BOX_STYLE
    ) {}
    def fetchReference(api: ReferenceData): RawReferenceData = ???

    `@pin`(
      "process_ref",
      schedule = Some("daily @ 09:00"),
      upstream = Set("ref_raw"),
      subgraph = Some("reference"),
      dotOptions = BOX_STYLE
    ) {}
    def processReference(raw: RawReferenceData): ProcessedReferenceData = ???

    `@pin`(
      "process_analytics",
      schedule = Some("hourly"),
      upstream = Set("analytics_raw", "market_analytics", "trades_aggregated"),
      subgraph = Some("analytics"),
      dotOptions = BOX_STYLE
    ) {}
    def processAnalytics(raw: AnalyticsData): ProcessedAnalyticsData = ???

    println(core.dot)
  }
}
