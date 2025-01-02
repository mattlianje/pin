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
}
