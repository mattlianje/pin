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
 graph TD
    %% Main diagram
    subgraph Flow
        direction TB
        etl["ETL"]
        basic-etl["Basic ETL"]
        advanced-etl["Advanced ETL"]
        db[(Database)]
        storage[("Storage")]
        
        %% Relationships - without labels
        etl --> basic-etl
        basic-etl --> advanced-etl
        etl --> advanced-etl
        
        %% Read/Write operations - without labels
        basic-etl -.-> db
        advanced-etl -.-> storage
        etl ==> storage
    end
    
    %% Style defs
    classDef default fill:#f9f9f9,stroke:#333,stroke-width:2px
    classDef storage fill:#ffebcc,stroke:#f90
    classDef database fill:#e6f3ff,stroke:#0066cc
    
    %% Styles
    class storage storage
    class db database
    
    %% Legend
    subgraph Legend
        direction LR
        l1[Process Node] --- l2[(Database)] --- l3[("Storage")]
        l4[" "] --->|"depends on"| l5[" "]
        l6[" "] -.->|"reads from"| l7[" "]
        l8[" "] ==>|"writes to"| l9[" "]
    end
 */
