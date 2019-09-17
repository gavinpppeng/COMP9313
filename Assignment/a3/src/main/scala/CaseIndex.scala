//COMP9313 Assignment3
//Author: Gavin
//Name: Wenxun Peng   ZID: z5195349
import java.io._
import scalaj.http._
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Set
import scala.util.parsing.json._
import scala.xml._
import sys.process._
import org.json4s._
import org.json4s.jackson.JsonMethods._

object CaseIndex{
    def main(args: Array[String]){
        //create index
        val index = Http("http://localhost:9200/legal_idx?pretty").method("PUT").header("Content-Type", "application/json").option(HttpOptions.connTimeout(30000)).option(HttpOptions.readTimeout(30000)).asString
        //create a new mapping
        val mapping = Http("http://localhost:9200/legal_idx/cases/_mapping?pretty").method("PUT").header("Content-Type", "application/json").postData("{\"cases\": {\"properties\": {\"name\": {\"type\":\"text\"},\"url\": {\"type\": \"text\"}, ,\"catchphrases\": {\"type\": \"text\"}, \"sentences\": {\"type\": \"text\"}, \"person\": {\"type\": \"text\"}, \"location\": {\"type\": \"text\"}, \"organiztion\": {\"type\": \"text\"}}}}").option(HttpOptions.connTimeout(30000)).option(HttpOptions.readTimeout(30000)).asString


        //load the input files
        val inputfile = args(0)
        //using a function to get all files in the directory
        val XML_list = getFiles(inputfile)
        XML_list.foreach(file => {
            println(file)
            //load every xml file
            val xml = XML.loadFile(file)
            //get the name
            var name = (xml \ "name").text
            //get the url
            var url = (xml \ "AustLII").text
            //get the catchphrases and remove all the space and use " " to replace "\n"
            var catchphrases = ((xml \ "catchphrases")).text.trim().replace("\n", " ")
            //get the sentences and remove all the space and use " " to replace "\n"
            var sentences = ((xml \ "sentences")).text.trim().replace("\n", " ")
            //add catchphrases and sentences together
            var make_all = (((xml \ "catchphrases")).text.trim() + "\n" + ((xml \ "sentences" \ "sentence")).text.trim()).split("\n")


            //create set to store each name entities
            //location entities
            val locations : Set[String] = Set()
            //people entities
            val people : Set[String] = Set()
            //organization entities
            val organizations : Set[String] = Set()
            //general terms
            val general_terms : Set[String] = Set()


            make_all.foreach(phrases => {
                if (phrases != "" && phrases != " "){
                    //request corenlp and handle the JSON file
                    var NLP_List = handleJSON(phrases)

                    //handle the JSON list file
                    NLP_List.foreach(text => {

                        //get the token format
                        var token = text.asInstanceOf[Map[String, String]]
                        //"originalText" attribute
                        var originalText = token.get("originalText")
                        // "ner" attribute
                        var ner = token.get("ner")
        

                        //change to String and then determin which set it belows
                        //People set
                        if ((ner match {case Some(string: String) => string}) == "PERSON") {                                                                                       
                            people += (originalText match {case Some(string: String) => string}).asInstanceOf[String]                                                                   
                        }
                        //Location set
                        else if ((ner match {case Some(string: String) => string}) == "LOCATION") {                                                                                          
                            locations += (originalText match {case Some(string: String) => string}).asInstanceOf[String]                                                                  
                        }
                        //Organization set
                        else if ((ner match {case Some(string: String) => string}) == "ORGANIZATION"){                                                                                 
                            organizations += (originalText match {case Some(string: String) => string}).asInstanceOf[String]                                                              
                        }

                    })

                }

            })

            //set the JSON format
            //set the "PERSON" attribute format
            val people_output = people.mkString(" ").replace("'", "\'").replace("\"", "\\\"")
            //set the "LOCATION" attribute format
            val locations_output = locations.mkString(" ").replace("'", "\'").replace("\"", "\\\"")
            //set the "ORGANIZATION" attribute format
            val organizations_output =  organizations.mkString(" ").replace("'", "\'").replace("\"", "\\\"")
            //set the "Sentences" format
            val new_sentence_output =  sentences.replace("'", "\'").replace("\"", "\\\"")
            //set the "Catchphrases" format
            val new_catchphrases_output = catchphrases.replace("'", "\'").replace("\"", "\\\"")
           
            //create a new document
            //set the format for the data to send
            val post_Data = "{\"name\":\"" + name.replace("'", "\'").replace("\"", "\\\"") + "\",\"url\":\"" + url.replace("'", "\'").replace("\"", "\\\"") + "\",\"catchphrases\":\"" + new_catchphrases_output + "\",\"sentences\":\"" + new_sentence_output + "\",\"person\":\"" + people_output + "\",\"location\":\"" + locations_output + "\",\"organization\":\"" + organizations_output + "\"}"
            //send the data and get the response
            val new_Response = Http("http://localhost:9200/legal_idx/cases/" + name.replace(" ", "%20") + "?pretty").method("PUT").header("Content-Type", "application/json").postData(post_Data).asString.body
        })

    }

    def getFiles(dir: String): List[File] = {
        val path = new File(dir)
        if (path.exists && path.isDirectory) {
            //get the ".xml" files and then print it in a list
            path.listFiles.filter(_.isFile).filter(f => f.toString.endsWith("xml")).toList
        } else {
            List[File]()
        }
  }

    def handleJSON(phrases: String): List[Any] = {
        //get the NLP result
        var NLP_result = Http("http://localhost:9000/?properties=%7B'annotators':'ner','outputFormat':'json'%7D").postData(phrases).method("POST").header("Content-Type", "application/json").option(HttpOptions.connTimeout(60000)).option(HttpOptions.readTimeout(60000)).asString.body
        
        //handle the JSON format to get the result
        //parse reponse to JSON object
        val NLP_json = JSON.parseFull(NLP_result)

        //get the "sentences" attributes
        val NLP_jsonObj = NLP_json match{
            case Some(map:Map[String, List[Any]]) => map.get("sentences")
        }

        //get "tokens" attributes
        val NLP_jsonObj_sub1 = NLP_jsonObj match {
            case Some(list:List[Map[String, Any]]) => list(0).get("tokens")
        }
    
        //get the list which satisfy the format(List[Map[String, String], Map[String, String]...])
        val NLP_jsonObj_sub2 = NLP_jsonObj_sub1 match {
            case Some(list:List[Map[String, String]]) => list
        }

        //put the NLP_jsonObj_sub2 in the list format
        val NLP_jsonObj_sub3 = NLP_jsonObj_sub2.asInstanceOf[List[Any]]
        return NLP_jsonObj_sub3
    }

}