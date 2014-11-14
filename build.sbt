name := "mozilla-telemetry"

version := "1.0"

libraryDependencies += "org.json4s" %% "json4s-native" % "3.2.11"

libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.2.11"

libraryDependencies += "net.databinder.dispatch" %% "dispatch-core" % "0.11.2"

libraryDependencies += "com.github.seratch" %% "awscala" % "0.4.+"

libraryDependencies += "org.apache.commons" % "commons-compress" % "1.9"

libraryDependencies += "org.tukaani" % "xz" % "1.5"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.1.0"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.1.0"
