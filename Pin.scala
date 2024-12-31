case class PinInfo(
  name: String,
  description: String = "",
  version: String = "1.0.0",



  tags: Set[String] = Set.empty,
  upstream: Set[String] = Set.empty
)

object pin {
  private var pins = Map.empty[String, PinInfo]
  
  def register(info: PinInfo): PinInfo = {
    println(s"📍 ${info.name}")
    if (info.version.nonEmpty) println(s"      version: v${info.version}")
    if (info.description.nonEmpty) println(s"      description: ${info.description}")
    if (info.tags.nonEmpty) println(s"      tags: ${info.tags.mkString(", ")}")
    if (info.upstream.nonEmpty) println(s"      upstream: ${info.upstream.mkString(", ")}")
    pins += (info.name -> info)
    info
  }
  
  def apply(
    name: String,
    description: String = "",
    version: String = "1.0.0",
    tags: Set[String] = Set.empty,
    upstream: Set[String] = Set.empty
  ): PinApplier = {
    new PinApplier(PinInfo(name, description, version, tags, upstream))
  }
  
  class PinApplier(meta: PinInfo) {
    def apply[A](value: A): A = {
      register(meta)
      value
    }
  }
  
  def all: Map[String, PinInfo] = pins
def mermaid: String = {
  val sb = new StringBuilder
  sb.append("graph TD\n")
  
  pins.values.foreach { pin =>
    sb.append(s"""    ${pin.name}["${pin.name}"]\n""")
  }

  pins.values.foreach { pin =>
    pin.upstream.foreach { up =>
      sb.append(s"""    ${up} --> ${pin.name}\n""")
    }
  }
  
  sb.toString
}
}

case class Pipeline(name: String, steps: List[String])

object Example {
  val p1 = Pipeline("etl-pipeline", List("extract", "transform", "load"))
  
  val pinnedP1 = pin(
    name = "etl",
    description = "ETL pipeline for data processing",
    version = "1.0.0",
    tags = Set("etl")
  ) {
    p1
  }

  val basicEtl: Pipeline = pin(
    name = "basic-etl",
    description = "Basic ETL Pipeline",
    tags = Set("etl", "basic"),
    upstream = Set("etl")
  ) {
    Pipeline("basic-etl", List("extract", "load"))
  }

  val advancedEtl = pin(
    name = "advanced-etl",
    description = "Advanced ETL Pipeline",
    tags = Set("etl", "advanced"),
    upstream = Set("basic-etl", "etl")
  ) {
    Pipeline("advanced-etl", List("extract", "transform", "load", "validate"))
  }

  def main(args: Array[String]): Unit = {
    println("\nMermaid Diagram:")
    println("```mermaid")
    println(pin.mermaid)
    println("```")
  }
}

/*
digraph ETL {
  # Default styling
  node [shape=box];
  edge [dir=forward];
  #rankdir=LR;
  
  subgraph cluster_0 {
    label = "Trading Data Pipeline";
    style=filled;
    color=lightgrey;
    
    trades_raw [shape=cylinder, label="trades_raw\nS3"];
    trades_processed [shape=cylinder, label="trades_processed\nS3", style=filled, fillcolor=peachpuff];
    trades_aggregated [shape=cylinder, label="trades_aggregated\nPostgres"];
    
    process_trades [label=<process_trades<BR/><FONT COLOR="#FF69B4" POINT-SIZE="10"><I>every 10m</I></FONT>>, style=filled, fillcolor=cyan2];
    aggregate_trades [label=<<B>aggregate_trades</B><BR/><FONT COLOR="#FF69B4" POINT-SIZE="10"><I>hourly</I></FONT>>];
    
    trades_raw -> process_trades -> trades_processed -> aggregate_trades -> trades_aggregated;
  }
  
  subgraph cluster_1 {
    label = "Market Data Pipeline";
    color=blue;
    
    market_feed [shape=cylinder, label="market_feed\nKafka"];
    market_processed [shape=cylinder, label="market_processed\nS3"];
    market_analytics [shape=cylinder, label="market_analytics\nRedshift"];
    
    ingest_market [label=<<B>ingest_market</B><BR/><FONT COLOR="#FF69B4" POINT-SIZE="10"><I>every 1m</I></FONT>>];
    analyze_market [label=<<B>analyze_market</B><BR/><FONT COLOR="#FF69B4" POINT-SIZE="10"><I>daily @ 00:00</I></FONT>>];
    
    market_feed -> ingest_market -> market_processed -> analyze_market -> market_analytics;
  }

  subgraph cluster_2 {
    label = "Reference Data Pipeline";
    color=green;
    
    ref_api [shape=cylinder, label="reference_api\nREST"];
    ref_raw [shape=cylinder, label="reference_raw\nS3"];
    ref_processed [shape=cylinder, label="reference_db\nPostgres"];
    
    fetch_ref [label=<<B>fetch_reference</B><BR/><FONT COLOR="#FF69B4" POINT-SIZE="10"><I>daily @ 08:00</I></FONT>>];
    process_ref [label=<<B>process_reference</B><BR/><FONT COLOR="#FF69B4" POINT-SIZE="10"><I>daily @ 09:00</I></FONT>>];
    
    ref_api -> fetch_ref -> ref_raw -> process_ref -> ref_processed;
  }

  subgraph cluster_3 {
    label = "Analytics Pipeline";
    color=purple;
    
    analytics_raw [shape=cylinder, label="analytics_raw\nKafka"];
    analytics_processed [shape=cylinder, label="analytics_dw\nSnowflake"];
    
    process_analytics [label=<<B>process_analytics</B><BR/><FONT COLOR="#FF69B4" POINT-SIZE="10"><I>hourly</I></FONT>>];
    
    analytics_raw -> process_analytics -> analytics_processed;
  }
  
  # Cross-pipeline dependencies
  trades_processed -> analyze_market;
  ref_processed -> process_trades;
  ref_processed -> analyze_market;
  trades_aggregated -> process_analytics;
  market_analytics -> process_analytics;
 }
 */
