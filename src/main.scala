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
class GossipNode extends Actor {

	val log = Logging(context.system, this)
	var rumourCount = 0
	def receive = {
	    case `rumour` => {
		    println(self.path.name + " received rumour.")
		    rumourCount = rumourCount + 1

		    if (rumourCount != 2){
                //send to a neighbor if counter not 10
                sender ! nextNeighbour
		    }
    		
	    }
	}
}

class MasterNode(numberOfNodes : Int) extends Actor {
	var nodesArray = new Array[ActorRef](numberOfNodes)
	val system = ActorSystem("HelloSystem")
    var topology = "line"
    
	//Topology for Full Network and for Line.
	for ( i <- 0 to numberOfNodes - 1) {
		var gossipNode = system.actorOf(Props[GossipNode], name = "gossipNode" + i)
		// add nodes to an array
		nodesArray(i) = gossipNode
	}

	def receive = {
		case `execute` => {
            //pick one random actor and start rumor.
            val randomActor = Random.shuffle(nodesArray.toList).head
		    randomActor ! rumour
	    }

		case `nextNeighbour` => {
			//fetch a random neighbor from the topology and send the rumor.
            var randomNeighbor = fetchNeighbour(sender)
            randomNeighbor ! rumour
		}
	}
    
    def fetchNeighbour(node : ActorRef) : ActorRef = {
        if(topology.equals("line")) {
            var currentIndex = nodesArray.indexOf(node)
            var leftNeighbor = nodesArray(currentIndex - 1)
            var rightNeighbor = nodesArray(currentIndex + 1)
            var adjacency = Array(leftNeighbor, rightNeighbor)
            
            val randomNeighbor = Random.shuffle(adjacency.toList).head
            return randomNeighbor
        }
        return null
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
