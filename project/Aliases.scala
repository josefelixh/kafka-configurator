import sbt._

object Aliases {

  lazy val defineCommandAliases = {
    addCommandAlias("ciBuild", ";clean; testOnly -- -l docker-compose; dockerComposeTest") ++
      addCommandAlias("ciRelease", ";clean; release with-defaults")
  }
}
