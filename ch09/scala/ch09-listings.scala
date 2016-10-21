
//section 9.1.1
import org.apache.spark.graphx._

case class Person(name:String, age:Int)
val vertices = sc.parallelize(Array((1L, Person("Homer", 39)),
    (2L, Person("Marge", 39)), (3L, Person("Bart", 12)),
    (4L, Person("Milhouse", 12))))
val edges = sc.parallelize(Array(Edge(4L, 3L, "friend"),
    Edge(3L, 1L, "father"), Edge(3L, 2L, "mother"),
    Edge(1L, 2L, "marriedTo")))

val graph = Graph(vertices, edges)
graph.vertices.count()
graph.edges.count()


//section 9.1.2
case class Relationship(relation:String)
val newgraph = graph.mapEdges((partId, iter) => iter.map(edge => Relationship(edge.attr)))
newgraph.edges.collect()
//Array[org.apache.spark.graphx.Edge[Relationship]] = Array(Edge(3,7,Relationship(girlfriend)), Edge(7,1,Relationship(father)), Edge(7,2,Relationship(mother)),
//Edge(1,6,Relationship(marriedTo)), Edge(4,1,Relationship(friendOf)), Edge(4,6,Relationship(friendOf)), Edge(5,4,Relationship(coworker)),
//Edge(2,4,Relationship(coworker)), Edge(5,2,Relationship(coworker)))

case class PersonExt(name:String, age:Int, children:Int=0, friends:Int=0, married:Boolean=false)
val newGraphExt = newgraph.mapVertices((vid, person) => PersonExt(person.name, person.age))

val aggVertices = newGraphExt.aggregateMessages(
    (ctx:EdgeContext[PersonExt, Relationship, Tuple3[Int, Int, Boolean]]) => {
        if(ctx.attr.relation == "marriedTo")
            { ctx.sendToSrc((0, 0, true)); ctx.sendToDst((0, 0, true)); }
        else if(ctx.attr.relation == "mother" || ctx.attr.relation == "father")
            { ctx.sendToDst((1, 0, false)); }
        else if(ctx.attr.relation == "friend")
            { ctx.sendToDst((0, 1, false)); ctx.sendToSrc((0, 1, false)); }
    },
    (msg1:Tuple3[Int, Int, Boolean], msg2:Tuple3[Int, Int, Boolean]) => (msg1._1+msg2._1, msg1._2+msg2._2, msg1._3 || msg2._3))
aggVertices.collect.foreach(println)
// (4,(0,1,false))
// (2,(1,0,true))
// (1,(1,0,true))
// (3,(0,1,false))

val graphAggr = newGraphExt.outerJoinVertices(aggVertices)((vid, origPerson, optMsg) => { optMsg match {
    case Some(msg) => PersonExt(origPerson.name, origPerson.age, msg._1, msg._2, msg._3)
    case None => origPerson
    }})
graphAggr.vertices.collect().foreach(println)
// (4,PersonExt(Milhouse,12,0,1,false))
// (2,PersonExt(Marge,39,1,0,true))
// (1,PersonExt(Homer,39,1,0,true))
// (3,PersonExt(Bart,12,0,1,false))

val parents = graphAggr.subgraph(_ => true, (vertexId, person) => person.children > 0)
parents.vertices.collect.foreach(println)
parents.edges.collect.foreach(println)

//section 9.2.1
val articles = sc.textFile("first-edition/ch09/articles.tsv", 6).filter(line => line.trim() != "" && !line.startsWith("#")).zipWithIndex().cache()
val links = sc.textFile("first-edition/ch09/links.tsv", 6).filter(line => line.trim() != "" && !line.startsWith("#"))
val linkIndexes = links.map(x => { val spl = x.split("\t"); (spl(0), spl(1)) }).join(articles).map(x => x._2).join(articles).map(x => x._2)
val wikigraph = Graph.fromEdgeTuples(linkIndexes, 0)
wikigraph.vertices.count()
//Long = 4592
articles.count()
//Long = 4604
linkIndexes.map(x => x._1).union(linkIndexes.map(x => x._2)).distinct().count()
//Long = 4592

//finding the articles that have no links
val distinctLinkIndexes = linkIndexes.map(x => x._1).union(linkIndexes.map(x => x._2)).distinct().map(x => (x, x))
articles.map(x => (x._2, x._1)).leftOuterJoin(distinctLinkIndexes).filter(x => x._2._2.isEmpty).collect()
//Array[(Long, (String, Option[Long]))] = Array((4333,(Vacutainer,None)), (1231,(Donation,None)), (1237,(Douglas_DC-4,None)), (441,(Badugi,None)), (3645,(Schatzki_ring,None)), (4545,(Wowpurchase,None)), (4480,(Wikipedia_Text_of_the_GNU_Free_Documentation_License,None)), (3352,(Private_Peaceful,None)), (3928,(Suikinkutsu,None)), (970,(Color_Graphics_Adapter,None)), (4289,(Underground_%28stories%29,None)), (2543,(Lone_Wolf_%28gamebooks%29,None)))

//section 9.2.2
articles.filter(x => x._1 == "Rainbow" || x._1 == "14th_century").collect().foreach(println)
// (14th_century,10)
// (Rainbow,3425)

import org.apache.spark.graphx.lib._
val shortest = ShortestPaths.run(wikigraph, Seq(10))
shortest.vertices.filter(x => x._1 == 3425).collect.foreach(println)
// (3425,Map(1772 -> 2))

//section 9.2.3
val ranked = wikigraph.pageRank(0.001)
val ordering = new Ordering[Tuple2[VertexId,Double]]{
    def compare(x:Tuple2[VertexId, Double], y:Tuple2[VertexId, Double]): Int = x._2.compareTo(y._2) }
val top10 = ranked.vertices.top(10)(ordering)
sc.parallelize(top10).join(articles.map(_.swap)).collect.sortWith((x, y) => x._2._1 > y._2._1).foreach(println)
// (4297,(43.064871681422574,United_States))
// (1568,(29.02695420077583,France))
// (1433,(28.605445025345137,Europe))
// (4293,(28.12516457691193,United_Kingdom))
// (1389,(21.962114281302206,English_language))
// (1694,(21.77679013455212,Germany))
// (4542,(21.328506154058328,World_War_II))
// (1385,(20.138550469782487,England))
// (2417,(19.88906178678032,Latin))
// (2098,(18.246567557461464,India))


//section 9.2.4
val wikiCC = wikigraph.connectedComponents()
wikiCC.vertices.map(x => (x._2, x._2)).distinct().join(articles.map(_.swap)).collect.foreach(println)
// (0,(0,%C3%81ed%C3%A1n_mac_Gabr%C3%A1in))
// (1210,(1210,Directdebit))
wikiCC.vertices.map(x => (x._2, x._2)).countByKey().foreach(println)
// (0,4589)
// (1210,3)

//section 9.2.5
val wikiSCC = wikigraph.stronglyConnectedComponents(100)
wikiSCC.vertices.map(x => x._2).distinct.count
// 519
wikiSCC.vertices.map(x => (x._2, x._1)).countByKey().filter(_._2 > 1).toList.sortWith((x, y) => x._2 > y._2).foreach(println)
// (6,4051)
// (2488,6)
// (1831,3)
// (892,2)
// (1950,2)
// (4224,2)
// (1111,2)
// (2474,2)
// (477,2)
// (557,2)
// (2160,2)
// (1834,2)
// (2251,2)
// (1513,2)
// (2142,2)
// (1986,2)
// (195,2)
// (1976,2)
// (2321,2)
wikiSCC.vertices.filter(x => x._2 == 2488).join(articles.map(x => (x._2, x._1))).collect.foreach(println)
// (2490,(2488,List_of_Asian_countries))
// (2496,(2488,List_of_Oceanian_countries))
// (2498,(2488,List_of_South_American_countries))
// (2493,(2488,List_of_European_countries))
// (2488,(2488,List_of_African_countries))
// (2495,(2488,List_of_North_American_countries))
wikiSCC.vertices.filter(x => x._2 == 1831).join(articles.map(x => (x._2, x._1))).collect.foreach(println)
// (1831,(1831,HD_217107))
// (1832,(1831,HD_217107_b))
// (1833,(1831,HD_217107_c))
wikiSCC.vertices.filter(x => x._2 == 892).join(articles.map(x => (x._2, x._1))).collect.foreach(println)
// (1262,(892,Dunstable_Downs))
// (892,(892,Chiltern_Hills))


//section 9.3
object AStar extends Serializable {
    import scala.reflect.ClassTag

    private val checkpointFrequency = 20

    /**
    * Computes shortest path between two vertices in a graph using the A* (A-star) algorithm.
    *
    * @tparam VD type of the vertex attribute
    * @tparam ED type of the edge attribute
    *
    * @graph the input graph for finding the shortest path
    * @origin origin vertex ID
    * @dest destination vertex ID
    * @maxIterations maximum number of iterations to perform
    * @estimateDistance function which calculates heuristical distance between two vertices
    * @edgeWeight function which returns weight of a given edge
    * @shouldVisitSource function which determines whether to visit source of the given vertex. Default is true for all edges.
    * @shouldVisitDestination function which determines whether to visit destination of the given vertex. Default is true for all edges.
    *
    * @return an array of vertex IDs on the shortest path, or an empty array if the path is not found.
    */
    def run[VD: ClassTag, ED: ClassTag](graph:Graph[VD, ED], origin:VertexId, dest:VertexId, maxIterations:Int = 100,
            estimateDistance:(VD, VD) => Double,
            edgeWeight:(ED) => Double,
            shouldVisitSource:(ED) => Boolean = (in:ED) => true,
            shouldVisitDestination:(ED) => Boolean = (in:ED) => true):Array[VD] = {

        val resbuf = scala.collection.mutable.ArrayBuffer.empty[VD]

        val arr = graph.vertices.flatMap(n => if(n._1 == origin || n._1 == dest) List[Tuple2[VertexId, VD]](n) else List()).collect()
        if(arr.length != 2)
             throw new IllegalArgumentException("Origin or destination not found")
        val origNode = if (arr(0)._1 == origin) arr(0)._2 else arr(1)._2
        val destNode = if (arr(0)._1 == origin) arr(1)._2 else arr(0)._2

        //The starting distance
        var dist = estimateDistance(origNode, destNode)

        case class WorkNode(origNode:VD, g:Double=Double.MaxValue, h:Double=Double.MaxValue, f:Double=Double.MaxValue, visited:Boolean=false, predec:Option[VertexId]=None)

        //initialize the graph
        var gwork = graph.mapVertices{ case(ind, node) => {
                if(ind == origin)
                    WorkNode(node, 0, dist, dist)
                else
                    WorkNode(node)
                }}.cache()

        var currVertexId:Option[VertexId] = Some(origin)
        var lastIter = 0
        for(iter <- 0 to maxIterations
            if currVertexId.isDefined; if currVertexId.getOrElse(Long.MaxValue) != dest)
        {
            lastIter = iter
            println("Iteration "+iter)

            //unpersist only vertices because we don't change the edges
            gwork.unpersistVertices()

            //Mark current vertex as visited
            gwork = gwork.mapVertices((vid:VertexId, v:WorkNode) => {
                if(vid != currVertexId.get)
                    v
                else
                    WorkNode(v.origNode, v.g, v.h, v.f, true, v.predec)
                }).cache()

            //to avoid expensive recomputations and stack overflow errors
            if(iter % checkpointFrequency == 0)
                gwork.checkpoint()

            //Calculate G, H and F for each neighbor
            val neighbors = gwork.subgraph(trip => trip.srcId == currVertexId.get || trip.dstId == currVertexId.get)
            val newGs = neighbors.aggregateMessages[Double](ctx => {
                    if(ctx.srcId == currVertexId.get && !ctx.dstAttr.visited && shouldVisitDestination(ctx.attr)) {
                        ctx.sendToDst(ctx.srcAttr.g + edgeWeight(ctx.attr))
                    }
                    else if(ctx.dstId == currVertexId.get  && !ctx.srcAttr.visited && shouldVisitSource(ctx.attr))
                    {
                        ctx.sendToSrc(ctx.dstAttr.g + edgeWeight(ctx.attr))
                    }}
                , (a1:Double, a2:Double) => a1, //never supposed to happen
                TripletFields.All)

            val cid = currVertexId.get
            gwork = gwork.outerJoinVertices(newGs)((nid, node, totalG) =>
                totalG match {
                    case None => node
                    case Some(newG) => {
                        if(node.h == Double.MaxValue) { // neighbor's H has not been calculated yet (it is not open, nor closed)
                            val h = estimateDistance(node.origNode, destNode)
                            WorkNode(node.origNode, newG, h, newG+h, false, Some(cid))
                        }
                        else if(node.h + newG < node.f) //the new f is less than the old
                        {
                            WorkNode(node.origNode, newG, node.h, newG+node.h, false, Some(cid))
                        }
                        else
                            node
                    }
                })

            //Find the vertex with the lowest F value from the open set (vertex is not visited and has an H value calculated)
            val openList = gwork.vertices.filter(v => v._2.h < Double.MaxValue && !v._2.visited)
            if(openList.isEmpty)
                currVertexId = None
            else
            {
                val nextV = openList.map(v => (v._1, v._2.f)).
                    reduce((n1, n2) => if(n1._2 < n2._2) n1 else n2)
                currVertexId = Some(nextV._1)
            }
        }//for

        if(currVertexId.isDefined && currVertexId.get == dest)
        {
            var currId:Option[VertexId] = Some(dest)
            var it = lastIter
            while(currId.isDefined && it >= 0)
            {
                val v = gwork.vertices.filter(x => x._1 == currId.get).collect()(0)
                resbuf += v._2.origNode
                currId = v._2.predec
                it = it - 1
            }
        }
        else
            println("Path not found!")
        gwork.unpersist()
        resbuf.toArray.reverse
    }//run
}

// section 9.3.3
case class Point(x:Double, y:Double, z:Double)

val vertices3d = sc.parallelize(Array((1L, Point(1,2,4)), (2L, Point(6,4,4)), (3L, Point(8,5,1)), (4L, Point(2,2,2)),
    (5L, Point(2,5,8)), (6L, Point(3,7,4)), (7L, Point(7,9,1)), (8L, Point(7,1,2)), (9L, Point(8,8,10)),
    (10L, Point(10,10,2)), (11L, Point(8,4,3)) ))
val edges3d = sc.parallelize(Array(Edge(1, 2, 1.0), Edge(2, 3, 1.0), Edge(3, 4, 1.0),
    Edge(4, 1, 1.0), Edge(1, 5, 1.0), Edge(4, 5, 1.0), Edge(2, 8, 1.0),
    Edge(4, 6, 1.0), Edge(5, 6, 1.0), Edge(6, 7, 1.0), Edge(7, 2, 1.0), Edge(2, 9, 1.0),
    Edge(7, 9, 1.0), Edge(7, 10, 1.0), Edge(10, 11, 1.0), Edge(9, 11, 1.0) ))
val graph3d = Graph(vertices3d, edges3d)

val calcDistance3d = (p1:Point, p2:Point) => {
    val x = p1.x - p2.x
    val y = p1.y - p2.y
    val z = p1.z - p2.z
    Math.sqrt(x*x + y*y + z*z)
}

val graph3dDst = graph3d.mapTriplets(t => calcDistance3d(t.srcAttr, t.dstAttr))

sc.setCheckpointDir("/tmp/sparkCheckpoint")

AStar.run(graph3dDst, 1, 10, 50, calcDistance3d, (e:Double) => e)
