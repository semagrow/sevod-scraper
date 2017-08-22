val sparkVersion = "1.5.1"
val jenaVersion = "3.0.0"

name := "sevod-scraper-rdf-spark"
version := "0.1-SNAPSHOT"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")
scalaVersion := "2.10.2"

resolvers += Resolver.mavenLocal

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.jena"  % "jena-core" % jenaVersion,
  "org.apache.jena"  % "jena-elephas-io" % jenaVersion,
  "org.apache.jena"  % "jena-arq" % jenaVersion//,
 // "com.hp.hpl.jena"  % "jena" % "2.6.4"
)

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => {
    case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
    case m if m.startsWith("META-INF") => MergeStrategy.discard
    case PathList("javax", "servlet", xs@_*) => MergeStrategy.first
    case PathList("org", "apache", xs@_*) => MergeStrategy.first
    case PathList("org", "jboss", xs@_*) => MergeStrategy.first
    case "about.html" => MergeStrategy.rename
    case "reference.conf" => MergeStrategy.concat
    case _ => MergeStrategy.first
  }
}
