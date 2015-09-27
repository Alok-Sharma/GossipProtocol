import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import akka.event.Logging

/**
 * @author Alok
 */
class GossipNode extends Actor {
  val log = Logging(context.system, this)
  def receive = {
    case "hello" => println("hello back at you")
    case _       => println("huh?")
  }
}

object Main {
  def main(args: Array[String]) : Unit = {
    var numOfNodes = args(0).toInt
    val system = ActorSystem("HelloSystem")
    val master = system.actorOf(Props[GossipNode], name = "master")
    
    for ( i <- 0 to numOfNodes) {
      val gossipNode = system.actorOf(Props[GossipNode], name = "gossipNode" + i)   
    }
  }
}