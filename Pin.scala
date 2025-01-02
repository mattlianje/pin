package pin

object core {
  sealed trait GraphNode {
    def name: String
    def upstream: Set[String]
    def dotOptions: Map[String, String]
  }

  case class Pin(
      name: String,
      description: Option[String] = None,
      schedule: Option[String] = None,
      version: String = "1.0.0",
      tags: Set[String] = Set.empty,
      upstream: Set[String] = Set.empty,
      dotOptions: Map[String, String] = Map.empty,
      subgraph: Option[String] = None
  ) extends GraphNode

  case class Subgraph(
      name: String,
      label: Option[String] = None,
      upstream: Set[String] = Set.empty,
      dotOptions: Map[String, String] = Map.empty,
      parent: Option[String] = None
  ) extends GraphNode

  private var nodes = Map.empty[String, GraphNode]

  def clear(): Unit = {
    nodes = Map.empty
  }

  private def formatLabel(
      name: String,
      description: Option[String],
      schedule: Option[String]
  ): String = {
    val desc = description
      .map(d => s"""<BR/><FONT POINT-SIZE="10">$d</FONT>""")
      .getOrElse("")
    val sched = schedule
      .map(s =>
        s"""<BR/><FONT COLOR="#FF69B4" POINT-SIZE="10"><I>$s</I></FONT>"""
      )
      .getOrElse("")
    s"""<$name$desc$sched>"""
  }

  private def formatOptions(
      options: Map[String, String],
      p: Option[Pin] = None
  ): String = {
    val baseOptions = options.toList
    val labelOption = p.flatMap(pin =>
      if (pin.description.isDefined || pin.schedule.isDefined)
        Some("label" -> formatLabel(pin.name, pin.description, pin.schedule))
      else None
    )
    if (baseOptions.isEmpty && labelOption.isEmpty) ""
    else
      s"[${(baseOptions ++ labelOption).map { case (k, v) => s"""$k=$v""" }.mkString(", ")}]"
  }

  def `@subgraph`(
      name: String,
      label: Option[String] = None,
      dotOptions: Map[String, String] = Map.empty,
      upstream: Set[String] = Set.empty,
      parent: Option[String] = None
  ): Unit = {
    nodes += (name -> Subgraph(name, label, upstream, dotOptions, parent))
  }

  def `@pin`[A](
      name: String,
      description: Option[String] = None,
      schedule: Option[String] = None,
      version: String = "1.0.0",
      tags: Set[String] = Set.empty,
      upstream: Set[String] = Set.empty,
      dotOptions: Map[String, String] = Map.empty,
      subgraph: Option[String] = None
  )(block: => A): A = {
    nodes += (name -> Pin(
      name,
      description,
      schedule,
      version,
      tags,
      upstream,
      dotOptions,
      subgraph
    ))
    block
  }

  private def writeSubgraph(
      sb: StringBuilder,
      name: String,
      indent: String = "  "
  ): Unit = {
    nodes.get(name) match {
      case Some(sg: Subgraph) =>
        sb.append(s"\n${indent}subgraph cluster_${name.replace("-", "_")} {\n")
        if (sg.label.nonEmpty)
          sb.append(s"""${indent}  label = "${sg.label.get}";\n""")
        sb.append(sg.dotOptions.map { case (k, v) =>
          s"""${indent}  $k="$v";\n"""
        }.mkString)

        nodes.values.collect {
          case s: Subgraph if s.parent.contains(name) =>
            writeSubgraph(sb, s.name, indent + "  ")
        }

        nodes.values.collect {
          case p: Pin if p.subgraph.contains(name) =>
            sb.append(
              s"${indent}  ${p.name.replace("-", "_")}${formatOptions(p.dotOptions, Some(p))};\n"
            )
        }

        sb.append(s"${indent}}\n")
      case _ =>
    }
  }

  def dot: String = {
    val sb = new StringBuilder
    sb.append("digraph G {\n")
    sb.append("  node [shape=box];\n")
    sb.append("  edge [dir=forward];\n")

    val standalonePins = nodes.values
      .collect {
        case p: Pin if p.subgraph.isEmpty => p
      }
      .toList
      .sortBy(_.name)
      .map { p =>
        s"  ${p.name.replace("-", "_")}${formatOptions(p.dotOptions, Some(p))};\n"
      }
    sb.append(standalonePins.mkString)

    nodes.values.collect {
      case s: Subgraph if s.parent.isEmpty => writeSubgraph(sb, s.name)
    }

    val edges = nodes.values
      .flatMap { case node: GraphNode =>
        node.upstream.toList.map(up =>
          s"  ${up.replace("-", "_")} -> ${node.name.replace("-", "_")};"
        )
      }
      .toList
      .sorted

    if (edges.nonEmpty) sb.append("\n" + edges.mkString("\n"))

    sb.append("\n}")
    sb.toString
  }
}

/* A good example ...
digraph ETL {
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
