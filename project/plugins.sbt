resolvers += Resolver.url("bintray-danosipov-sbt-plugin-releases",
  url("http://dl.bintray.com/content/danosipov/sbt-plugins"))(Resolver.ivyStylePatterns)

addSbtPlugin("com.danosipov" % "sbt-scalding-plugin" % "1.0.1")

resolvers += "sbt-plugin-releases" at "http://repo.scala-sbt.org/scalasbt/sbt-plugin-releases"

addSbtPlugin("com.cavorite" % "sbt-avro" % "0.3.2")
