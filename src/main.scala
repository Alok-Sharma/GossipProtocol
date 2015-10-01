import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import akka.event.Logging
import akka.actor.ActorRef
import scala.util.Random

case object rumour
case object execute
case object nextNeighbour

/**
 * @author Alok
 */
class GossipNode(masterNode : MasterNode) extends Actor {
	val log = Logging(context.system, this)

			def receive = {
			case `rumour` => {
				println(self.path.name + " received rumour.")
        // increment counter
        //send to a neighbour
			}
	}
}

class MasterNode(numberOfNodes : Int) extends Actor {
  val system = ActorSystem("HelloSystem")
  var nodesArray = new Array[ActorRef](numberOfNodes)
  
  //Topology for Full Network and for Line.
	for ( i <- 0 to numberOfNodes - 1) {
		var gossipNode = system.actorOf(Props(new GossipNode(this)), name = "gossipNode" + i)
		// add nodes to an array
		nodesArray(i) = gossipNode
	}

	//pick one random actor and send rumour.
	val randomActor = Random.shuffle(nodesArray.toList).head
	def receive = {
    case `execute` => {
      randomActor ! rumour
		}
    
    case `nextNeighbour` => {
      //fetch a random neighbour from the topology and send the rumour.
    }
	}
}

object Main {
	def main(args: Array[String]) : Unit = {
			var numOfNodes = args(0).toInt
			val system = ActorSystem("HelloSystem")
			var masterNode = system.actorOf(Props(new MasterNode(numOfNodes)), name = "master")
      
      masterNode ! execute
	}
}
