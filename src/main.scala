import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import akka.event.Logging
import akka.actor.ActorRef
import scala.util.Random

case object rumour
case object execute
case object nextNeighbour
case class pushSum(s :Double, w :Double)
case object done

class GossipNode(topology : String) extends Actor {
    
	//following variables are for push-sum only
	var s = 0.0	//initial sum of each node for push-sum
	var w = 1.0	//initial weight
    var ratio = s/w	
	var rumourCount = 0
    var ratiosArray = new Array[Double](2)
    ratiosArray(0) = 0
    ratiosArray(1) = 0
    
    if(topology.equals("3D") || topology.equals("imp3D")) {
        var coords = self.path.name.split(":")
        val x = coords(1).toInt
        val y = coords(2).toInt
        val z = coords(3).toInt
        val nodeCount = coords(4).toInt
        
        s = nodeCount.toDouble + 1.0
    } else{
        s = self.path.name.substring(10).toDouble + 1.0
    }
    
	def receive = {
		//For gossip
		case `rumour` => {
			rumourCount = rumourCount + 1
			if (rumourCount != 10){
				sender ! nextNeighbour
			} else {
                sender ! done
                context.stop(self)
            }
		}
        
		//For Push sum
        case pushSum(receivedS, receivedW) => {
        	
            rumourCount = rumourCount + 1
            s = s + receivedS
            w = w + receivedW
            ratio = s/w
            var oldRatio1 = ratiosArray(0)
            var oldRatio2 = ratiosArray(1)
//          println("s: " + s + " w:" + w + " ratio: " + ratio + " old1: " + oldRatio1 + " old2: " + oldRatio2 + "\n")
            
            //if not converging
            if(Math.abs(ratio - oldRatio1) > math.pow(10, -10) || Math.abs(ratio - oldRatio2) > math.pow(10, -10)) {
                if (rumourCount % 2 == 0) {
                    ratiosArray(0) = ratio
                } else {
                    ratiosArray(1) = ratio
                }
                s = s/2
                w = w/2
                sender ! pushSum(s, w)
            } else {
                println("Ratio: " + ratio)
                sender ! done
                context.stop(self)
            }
        }
	}
}

class MasterNode(numberOfNodes : Int, topology : String, algo : String) extends Actor {
	var nodesArray = new Array[ActorRef](numberOfNodes)

	//For 3D grid, we need to round up to the nearest cube of a number. 
	//I'm finding the cube root and rounding up to get no. of rows in one dimension. 
	var cbrt = math.cbrt(numberOfNodes)
	cbrt = math.ceil(cbrt)
	var nodesArray3D = Array.ofDim[ActorRef](cbrt.toInt, cbrt.toInt, cbrt.toInt)
	
	val system = ActorSystem("HelloSystem")
	buildTopology()    
	val startTime = System.currentTimeMillis
	println("Start time: " + startTime)
	def receive = {
		case `execute` => {
			//pick one random actor and start rumor.
            var randomActor : ActorRef = null
            if(topology.equals("line") || topology.equals("full")) {
                randomActor = Random.shuffle(nodesArray.toList).head
            } else if(topology.equals("3D") || topology.equals("imp3D")) {
                randomActor = Random.shuffle(nodesArray3D.toList).head.toList.head.toList.head
            }
            
            if(algo.equals("gossip")) {
                randomActor ! rumour
            } else {
                randomActor ! pushSum(0,0)
            }
		}

		case `nextNeighbour` => {
			//fetch a random neighbor from the topology and send the rumor.
			var randomNeighbor = fetchNeighbour(sender)
			randomNeighbor ! rumour
		}
        
        case pushSum(receivedS, receivedW) => {
            var randomNeighbor = fetchNeighbour(sender)
            randomNeighbor ! pushSum(receivedS, receivedW)
        }
        
        case `done` => {
        	val endTime = System.currentTimeMillis
        	val timeTaken = endTime - startTime
        	println("Time taken :" + timeTaken)
        	context.system.shutdown()        	
        }
	}
    
   def buildTopology() : Unit = {
        if(topology.equals("line") || topology.equals("full")) {
            for ( i <- 0 to numberOfNodes - 1) {
                var gossipNode = system.actorOf(Props(new GossipNode(topology)), name = "gossipNode" + i)
                // add nodes to an array
                nodesArray(i) = gossipNode
            }
        } else if (topology.equals("3D") || topology.equals("imp3D")){
            var nodeCount = 0
            for(i<- 0 to cbrt.toInt - 1) {
                for(j <- 0 to cbrt.toInt - 1) {
                    for(k <- 0 to cbrt.toInt - 1) {
                        var gossipNode = system.actorOf(Props(new GossipNode(topology)), name = "gossipNode:" + i + ":" + j + ":" + k + ":" + nodeCount)
                        nodesArray3D(i)(j)(k) = gossipNode
                        nodeCount = nodeCount + 1
//                        println(gossipNode.path.name)
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
        } else if (topology.equals("full")) {
            var adjacencyArray = nodesArray.filter { (x:ActorRef) => x != node }
            val randomNeighbor = Random.shuffle(adjacencyArray.toList).head
            return randomNeighbor
        } else if (topology.equals("3D") || topology.equals("imp3D")) {
            var adjacencyArray = new Array[ActorRef](7)
            var coords = node.path.name.split(":")
            val curX = coords(1).toInt
            val curY = coords(2).toInt
            val curZ = coords(3).toInt

            //x-1, x+1, y-1, y+1, z-1, z+1
            if (curX - 1 >= 0) {
                adjacencyArray(0) = nodesArray3D(curX - 1)(curY)(curZ)
            }
            if (curX + 1 < cbrt) {
                adjacencyArray(1) = nodesArray3D(curX + 1)(curY)(curZ)
            }
            if (curY - 1 >= 0) {
                adjacencyArray(2) = nodesArray3D(curX)(curY - 1)(curZ)
            }
            if(curY + 1 < cbrt) { 
                adjacencyArray(3) = nodesArray3D(curX)(curY + 1)(curZ)
            }
            if(curZ - 1 >= 0) {
                adjacencyArray(4) = nodesArray3D(curX)(curY)(curZ - 1)
            }
            if(curZ + 1 < cbrt) {
                adjacencyArray(5) = nodesArray3D(curX)(curY)(curZ + 1)
            }
            if(topology.equals("imp3D")) {
                adjacencyArray(6) = Random.shuffle(nodesArray3D.toList).head.toList.head.toList.head
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
        var topology = args(1)
        var algo = args(2)
        println("number of Node:" + numOfNodes + " topology:" + topology + " algo:" + algo)
	    val system = ActorSystem("HelloSystem")
		var masterNode = system.actorOf(Props(new MasterNode(numOfNodes, topology, algo)), name = "master")
		masterNode ! execute
	}
}
