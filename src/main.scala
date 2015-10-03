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

	//For 3D grid, we need to round up to the nearest cube of a number. 
	//I'm finding the cube root and rounding up to get no. of rows in one dimension. 
	var cbrt = math.cbrt(numberOfNodes)
	println(cbrt)
	cbrt = math.ceil(cbrt) 
	println(cbrt)
	var nodesArray3D = Array.ofDim[ActorRef](cbrt.toInt, cbrt.toInt, cbrt.toInt)
	
	val system = ActorSystem("HelloSystem")
	var topology = "3D"
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
		if(topology.equals("line")) {
			for ( i <- 0 to numberOfNodes - 1) {
				var gossipNode = system.actorOf(Props[GossipNode], name = "gossipNode" + i)
				// add nodes to an array
				nodesArray(i) = gossipNode
			}
		} else if (topology.equals("3D") || topology.equals("imp3D")){
			for(i<- 0 to cbrt.toInt-1){
				for(j <- 0 to cbrt.toInt-1){
					for(k <- 0 to cbrt.toInt-1){
						var gossipNode = system.actorOf(Props[GossipNode], name = "gossipNode" + i + j+ k)
						nodesArray3D(i)(j)(k) = gossipNode
						println(gossipNode.path.name)
					}
				}
			}
		}
	}

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
			} else if(topology.equals("full")) {
				var adjacencyArray = nodesArray.filter { (x:ActorRef) => x != node }
				val randomNeighbor = Random.shuffle(adjacencyArray.toList).head
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
