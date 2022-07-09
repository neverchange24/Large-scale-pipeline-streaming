import org.apache.kafka.clients.producer._
import java.util._
import org.apache.kafka.common.serialization._
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.log4j.{LogManager, Logger}
import org.apache.kafka.common.security.auth.SecurityProtocol.SASL_PLAINTEXT  //pour la securité//



object Producerkafka extends App {

  private val trace_kafka = LogManager.getLogger("console")
    def getProducerkafka(message: String): Unit = {

    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.ACKS_CONFIG,"all")
    props.put("security.protocol","SASL_PLAINTEXT")

    val topic: String = ""

    try{
      val tuitterProducer = new KafkaProducer [String, String](props)
      val tuitterRecord = new ProducerRecord [String, String](topic, message)
      tuitterProducer.send(tuitterRecord)

    }
    catch {
      case ex: Exception => trace_kafka.error(s"erreur dans l'envoi du message. Details de l'erreur: ${ex.printStackTrace()}")

    }
  }


}
