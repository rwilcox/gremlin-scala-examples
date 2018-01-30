import gremlin.scala._
import org.scalatest.{Matchers, WordSpec}
import org.janusgraph.core.JanusGraphFactory
import org.janusgraph.core.JanusGraph
import org.apache.commons.configuration.BaseConfiguration
import org.apache.tinkerpop.gremlin.driver.Client.ClusteredClient
import org.apache.tinkerpop.gremlin.driver.Cluster
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnection
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;

class SimpleSpec extends WordSpec with Matchers {

  "janusgraph server ported Java (from janusgraph-server example)" in {
    // used for bindings
    val NAME: String = "name"
    val AGE: String = "age"
    val TIME: String = "time"
    val REASON: String = "reason"
    val PLACE: String = "place"
    val LABEL: String = "label"
    val OUT_V: String = "outV"
    val IN_V: String = "inV"

    import org.apache.tinkerpop.gremlin.process.traversal.Bindings
    val b = Bindings.instance
    val conf = new BaseConfiguration()
    //conf.setProperty("storage.backend","inmemory")
    conf.setProperty("gremlin.remote.remoteConnectionClass", "org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection")
    conf.setProperty("gremlin.remote.driver.clusterFile", "src/test/resources/gremlin_server.yml")
    conf.setProperty("gremlin.remote.driver.sourceName", "g")

    val cluster = Cluster.open("src/test/resources/gremlin_server.yml")
    val client : ClusteredClient = cluster.connect()

    val graph = JanusGraphFactory.open("inmemory")
    val g = graph.traversal().withRemote( conf )

    import org.janusgraph.core.attribute.Geoshape
    val saturn = g.addV(b.of(LABEL, "titan")).property(NAME, b.of(NAME, "saturn")).property(AGE, b.of(AGE, 10000)).next()
    val sky = g.addV(b.of(LABEL, "location")).property(NAME, b.of(NAME, "sky")).next()
    val sea = g.addV(b.of(LABEL, "location")).property(NAME, b.of(NAME, "sea")).next()
    val jupiter = g.addV(b.of(LABEL, "god")).property(NAME, b.of(NAME, "jupiter")).property(AGE, b.of(AGE, 5000)).next()
    val neptune = g.addV(b.of(LABEL, "god")).property(NAME, b.of(NAME, "neptune")).property(AGE, b.of(AGE, 4500)).next()
    val hercules = g.addV(b.of(LABEL, "demigod")).property(NAME, b.of(NAME, "hercules")).property(AGE, b.of(AGE, 30)).next()
    val alcmene = g.addV(b.of(LABEL, "human")).property(NAME, b.of(NAME, "alcmene")).property(AGE, b.of(AGE, 45)).next()
    val pluto = g.addV(b.of(LABEL, "god")).property(NAME, b.of(NAME, "pluto")).property(AGE, b.of(AGE, 4000)).next()
    val nemean = g.addV(b.of(LABEL, "monster")).property(NAME, b.of(NAME, "nemean")).next()
    val hydra = g.addV(b.of(LABEL, "monster")).property(NAME, b.of(NAME, "hydra")).next()
    val cerberus = g.addV(b.of(LABEL, "monster")).property(NAME, b.of(NAME, "cerberus")).next()
    val tartarus = g.addV(b.of(LABEL, "location")).property(NAME, b.of(NAME, "tartarus")).next()

    g.close()
    cluster.close()
  }


  "janusgraph server gremlin-scala-example" in {
    val conf = new BaseConfiguration()
    //conf.setProperty("storage.backend","inmemory")
    conf.setProperty("gremlin.remote.remoteConnectionClass", "org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection")
    conf.setProperty("gremlin.remote.driver.clusterFile", "src/test/resources/gremlin_server.yml")
    conf.setProperty("gremlin.remote.driver.sourceName", "g")
    
    val cluster = Cluster.open("src/test/resources/gremlin_server.yml")
    val client : ClusteredClient = cluster.connect()
    
    //val graph = EmptyGraph.instance().asScala().configure(_.withRemote( conf ) )

    val graph : ScalaGraph = JanusGraphFactory.open("inmemory").configure( _.withRemote( conf) )
    //val graph : ScalaGraph = JanusGraphFactory.open("inmemory").traversal().configure
    //val g = graph.traversal.withRemote( conf )

    //val remoteGraph = graph.traversal().withRemote( conf );
    //val scalaGraph = graph.asScala

    val Name = Key[String]("name")
    val Planet = "planet"
    val Saturn = "saturn"

    (1 to 4) foreach { i ⇒
      graph + (Planet, Name -> s"vertex $i")
    }
    val saturnV : Vertex = graph + (Saturn, Name -> Saturn)
    val sunV    : Vertex = graph + ("sun", Name -> "sun")
    val edge    : Edge = saturnV --- "orbits" --> sunV


    //graph.tx().commit() // does not work

    graph.V.count.head shouldBe 6
    graph.E.count.head shouldBe 1

    val traversal = graph.V.value(Name)
    traversal.toList.size shouldBe 6

    graph.V.hasLabel(Saturn).count.head shouldBe 1

    val saturnQ = graph.V.hasLabel(Saturn).head
    saturnQ.value2(Name) shouldBe Saturn

	//g.close()
	cluster.close()
    //graph.close
  }

  "janusgraph server Raw Gremlin (poorly ported from gremlin-scala)" in {
    val conf = new BaseConfiguration()
    //conf.setProperty("storage.backend","inmemory")
    conf.setProperty("gremlin.remote.remoteConnectionClass", "org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection")
    conf.setProperty("gremlin.remote.driver.clusterFile", "src/test/resources/gremlin_server.yml")
    conf.setProperty("gremlin.remote.driver.sourceName", "g")

    val cluster = Cluster.open("src/test/resources/gremlin_server.yml")
    val client : ClusteredClient = cluster.connect()

    //val traversalSource : GraphTraversalSource = JanusGraphFactory.open("inmemory").traversal().withRemote( DriverRemoteConnection.using(cluster, "g") )
    //val graph = new ScalaGraph( new TraversalSource(traversalSource))

    val graph = JanusGraphFactory.open("inmemory").traversal().withRemote( DriverRemoteConnection.using(cluster, "g") )
      //asScala().configure(_.withRemote( conf ) )

    //val graph : ScalaGraph = JanusGraphFactory.open("inmemory").configure( _.withRemote( conf) )
    //val graph : ScalaGraph = JanusGraphFactory.open("inmemory").traversal().configure
    //val g = graph.traversal.withRemote( conf )

    //val remoteGraph = graph.traversal().withRemote( conf );
    //val scalaGraph = graph.asScala

    val Name = Key[String]("name")
    val Planet = "planet"
    val Saturn = "saturn"

    (1 to 4) foreach { i ⇒
      graph.addV().property("name", s"vertex $i")
      //graph + (Planet, Name -> s"vertex $i")
    }

    val saturnV = graph.addV().property("name", Saturn)
    //val saturnV = graph + (Saturn, Name -> Saturn)
    val sunV = graph.addV().property("name", "sun")
    //val sunV = graph + ("sun", Name -> "sun")

    saturnV.addE("orbits").addV("sun").next()  // addV returns GraphTraversal<S, Vertex> ie with the add vertex step added
    //saturnV --- "orbits" --> sunV


    //graph.tx().commit() // does not work

    //graph.V.count.head shouldBe 6
    //graph.E.count.head shouldBe 1

    //val traversal = graph.V.value(Name)
    //traversal.toList.size shouldBe 6

    //graph.V.hasLabel(Saturn).count.head shouldBe 1

    //val saturnQ = graph.V.hasLabel(Saturn).head
    //saturnQ.value2(Name) shouldBe Saturn

    graph.close()
    cluster.close()
    //graph.close
  }
}
