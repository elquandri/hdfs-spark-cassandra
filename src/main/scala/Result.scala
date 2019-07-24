
import com.google.gson.Gson
import com.typesafe.config.{Config, ConfigFactory}


case class Attachments(
                        title: String,
                        description: String,
                        fileName: String,
                        contentType: String
                      )
case class result(
                           testCase: String,
                           notes: String,
                           plateform: String,
                           testProject: String,
                           testPlan: String,
                           build: String,
                           result: String,
                           attachments: Attachments
                         )

object resultcheck extends App  {


  val co:Config=ConfigFactory.load("Cassandra.conf")
  println("test" +co.getString("connection_host"))

  def generate(status: String): Object = status match {

    case "passed" =>
      val properties: Config = ConfigFactory.load("integration.conf")
      val testCase = properties.getString("passed.testCase")
      val notes = properties.getString("passed.notes")
      val plateform = properties.getString("passed.plateform")
      val testProjet = properties.getString("passed.testProject")
      val testPlan = properties.getString("passed.testPlan")
      val build = properties.getString("passed.build")
      val res = properties.getString("passed.result")
      val title = properties.getString("passed_attachments.title")
      val desc = properties.getString("passed_attachments.description")
      val filename = properties.getString("passed_attachments.fileName")
      val content = properties.getString("passed_attachments.contentType")
      val fileres = result(
        testCase, notes, plateform, testProjet, testPlan, build, res
        , Attachments(title, desc, filename, content))
      val gson = new Gson()
      val jsonString = gson.toJson(fileres)
      return jsonString

    case "failed" =>
      val properties: Config = ConfigFactory.load("integration.conf")
      val testCase = properties.getString("failed.testCase")
      val notes = properties.getString("failed.notes")
      val plateform = properties.getString("failed.plateform")
      val testProjet = properties.getString("failed.testProject")
      val testPlan = properties.getString("failed.testPlan")
      val build = properties.getString("failed.build")
      val res = properties.getString("failed.result")
      val title = properties.getString("failed_attachments.title")
      val desc = properties.getString("failed_attachments.description")
      val filename = properties.getString("failed_attachments.fileName")
      val content = properties.getString("failed_attachments.contentType")
      val fileres = result(
        testCase, notes, plateform, testProjet, testPlan, build, res
        , Attachments(title, desc, filename, content))
      val gson = new Gson()
      val jsonString = gson.toJson(fileres)
      return jsonString

    case "bloqued" =>
      val properties: Config = ConfigFactory.load("integration.conf")
      val testCase = properties.getString("bloqued.testCase")
      val notes = properties.getString("bloqued.notes")
      val plateform = properties.getString("bloqued.plateform")
      val testProjet = properties.getString("bloqued.testProject")
      val testPlan = properties.getString("bloqued.testPlan")
      val build = properties.getString("bloqued.build")
      val res = properties.getString("bloqued.result")
      val title = properties.getString("bloqued_attachments.title")
      val desc = properties.getString("bloqued_attachments.description")
      val filename = properties.getString("bloqued_attachments.fileName")
      val content = properties.getString("bloqued_attachments.contentType")
      val fileres = result(
        testCase, notes, plateform, testProjet, testPlan, build, res
        , Attachments(title, desc, filename, content))

      val gson = new Gson()
      val jsonString = gson.toJson(fileres)
      return jsonString
  }
}