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

		    if (rumourCount != 4){
                //send to a neighbor
                sender ! nextNeighbour
		    }
	    }
	}
}

class MasterNode(numberOfNodes : Int) extends Actor {
	var nodesArray = new Array[ActorRef](numberOfNodes)
	val system = ActorSystem("HelloSystem")
    var topology = "3d"
    buildTopology()    

    println("inside master")
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
    
    def buildTopology() : Unit = {
	    if(topology.equals("line") || topology.equals("full")) {
		    for ( i <- 0 to numberOfNodes - 1) {
			    var gossipNode = system.actorOf(Props[GossipNode], name = "gossipNode" + i)
				// add nodes to an array
				nodesArray(i) = gossipNode
		    }
	    }
        //add more for 3D grids
    }
    
    //test 3d array
    val cubeRoot = 5
    val x, y, z = cubeRoot
    val testArray = Array.ofDim[ActorRef](x,y,z)
    
    def fetchNeighbour(node : ActorRef) : ActorRef = {
        if(topology.equals("line")) {
            var currentIndex = nodesArray.indexOf(node)
            var leftIndex = currentIndex - 1
            var rightIndex = currentIndex + 1
            if (currentIndex == 0) {
                var rightNeighbor = nodesArray(currentIndex + 1) //will fail if only one element.
                return rightNeighbor
            } else if (currentIndex == numberOfNodes - 1) {
                var leftNeighbor = nodesArray(currentIndex - 1)
                return leftNeighbor
            } else {
                var leftNeighbor = nodesArray(currentIndex - 1)
                var rightNeighbor = nodesArray(currentIndex + 1)
                var adjacencyArray = Array(leftNeighbor, rightNeighbor)
                val randomNeighbor = Random.shuffle(adjacencyArray.toList).head
                return randomNeighbor
            }
        } else if (topology.equals("full")) {
            var adjacencyArray = nodesArray.filter { (x:ActorRef) => x != node }
            val randomNeighbor = Random.shuffle(adjacencyArray.toList).head
            return randomNeighbor
        } else if (topology.equals("3d")) {
            var adjacencyArray = new Array[ActorRef](6)
            var nodeName = node.path.name
            var curX = nodeName.substring(10, 11).toInt
            var curY = nodeName.substring(11, 12).toInt
            var curZ = nodeName.substring(12, 13).toInt

            //x-1, x+1, y-1, y+1, z-1, z+1
            if (curX - 1 >= 0) {
                adjacencyArray(0) = testArray(curX - 1)(curY)(curZ)
            }
            if (curX + 1 < cubeRoot) {
                adjacencyArray(1) = testArray(curX + 1)(curY)(curZ)
            }
            if (curY - 1 >= 0) {
                adjacencyArray(2) = testArray(curX)(curY - 1)(curZ)
            }
            if(curY + 1 < cubeRoot) { 
                adjacencyArray(3) = testArray(curX)(curY + 1)(curZ)
            }
            if(curZ - 1 >= 0) {
                adjacencyArray(4) = testArray(curX)(curY)(curZ - 1)
            }
            if(curZ + 1 < cubeRoot) {
                adjacencyArray(5) = testArray(curX)(curY)(curZ + 1)
            }
            
            var randomNeighbor : ActorRef = null
            while(randomNeighbor == null) {
                randomNeighbor = Random.shuffle(adjacencyArray.toList).head
            }
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
