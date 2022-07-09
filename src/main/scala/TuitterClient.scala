import com.twitter.hbc.httpclient.auth._
import com.twitter.hbc.core.{Client, Constants}
import com.twitter.hbc.core.endpoint.{StatusesFilterEndpoint, Location}
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue, TimeUnit}
import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import scala.collection.JavaConverters._
import org.apache.log4j.{LogManager, Logger}
import Producerkafka._

class TuitterClient {

  private val trace_hbc = LogManager.getLogger("console")

  def getClient_Tuitter(token:String,consumersecret:String,consumerkey:String,tokensecret:String, town_list: Location ) : Unit= {

    val queue: BlockingQueue[String] = new LinkedBlockingQueue[String](1000000)
    val auth: Authentication = new OAuth1(consumerkey, consumersecret, token, tokensecret)
    val endp: StatusesFilterEndpoint = new StatusesFilterEndpoint()
    endp.locations(List(town_list).asJava)

    val client_HBC = new ClientBuilder()
      .hosts(Constants.STREAM_HOST)
      .authentication(auth)
      .gzipEnabled(true)
      .endpoint(endp)
      .processor(new StringDelimitedProcessor(queue))

    val client_HBC_complete = client_HBC.build()
    client_HBC_complete.connect()

    try{
      while(client_HBC_complete.isDone) {
        val tweet = queue.poll(300, TimeUnit.SECONDS)
        getProducerkafka(tweet)

      }
    } catch {
        case ex: Exception => trace_hbc.error(s"Erreur dans le client HBC. Details: ${ex.printStackTrace()}")
      } finally {
        client_HBC_complete.stop()
      }

  }
}
