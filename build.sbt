val name = "beam-template"
val scalaV = "2.12.4"

lazy val root: Project =
  Project(name, file("."))
    .settings(scalaVersion := scalaV)
    .settings(scalariformSupportformatSettings)
    .settings(
      libraryDependencies ++= Seq(
        "com.google.cloud.dataflow" % "google-cloud-dataflow-java-sdk-all" % "2.1.0",
        "org.slf4j" % "slf4j-api" % "1.7.25",
        "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
        "ch.qos.logback" % "logback-classic" % "1.2.3",
        "com.typesafe" % "config" % "1.3.2",
      ))

import scalariform.formatter.preferences._
def formattingPreferences =
  FormattingPreferences()
    .setPreference(RewriteArrowSymbols, true)
    .setPreference(AlignParameters, true)
    .setPreference(AlignSingleLineCaseStatements, true)
    .setPreference(DoubleIndentConstructorArguments, true)

lazy val scalariformSupportformatSettings = SbtScalariform.scalariformSettings ++ Seq(
  SbtScalariform.ScalariformKeys.preferences in Compile := formattingPreferences,
  SbtScalariform.ScalariformKeys.preferences in Test    := formattingPreferences
)