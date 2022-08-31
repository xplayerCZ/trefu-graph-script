import cz.davidkurzica.model.Lines
import cz.davidkurzica.model.RouteStops
import cz.davidkurzica.model.Routes
import model.*
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.select
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.TransactionManager
import org.jetbrains.exposed.sql.transactions.experimental.newSuspendedTransaction
import java.sql.Connection
import java.sql.ResultSet

fun <T:Any> String.execAndMap(transform : (ResultSet) -> T) : List<T> {
    val result = arrayListOf<T>()
    TransactionManager.current().exec(this) { rs ->
        while (rs.next()) {
            result += transform(rs)
        }
    }
    return result
}

suspend fun <T> dbQuery(
    block: suspend () -> T,
): T = newSuspendedTransaction(transactionIsolation = Connection.TRANSACTION_REPEATABLE_READ) { block() }

fun createQueryString(routeId: Int): String {
    return """
        SELECT pairs."stop", pairs."next_stop"
        FROM (
            SELECT
                stops."stop_id" AS "stop",
                LEAD(stops."stop_id") OVER (ORDER BY index) AS "next_stop"
            FROM route_stops
            JOIN stops ON stops.stop_id = route_stops.stop_id 
            WHERE route_stops.route_id = $routeId
        ) as "pairs"
        WHERE "next_stop" IS NOT NULL
    """.trimIndent()
}

fun selectRouteStopsPairs(routeId: Int): List<Pair<Int, Int>> {
    return createQueryString(routeId).execAndMap { rs ->
        rs.getInt("stop") to rs.getInt("next_stop")
    }
}

fun Pair<Int, Int>.replaceStopIdsForNodeIds(nodes: List<Node>, graphId: Int): Pair<Int, Int> {
    val filteredNodes = nodes.filter { it.graphId == graphId  }
    filteredNodes.groupBy { it.stopId }.forEach { map ->
        map.value.forEach {
            print("${map.key} $it")
        }
    }
    val first = filteredNodes.single { it.stopId == this.first }.id
    val second = filteredNodes.single { it.stopId == this.second }.id
    return Pair(first, second)
}

fun createGraph(packetId: Int) {
    val graphId = (Graphs.insert {
        it[Graphs.packetId] = packetId
    } get Graphs.id)

    val stopIds = (Stops innerJoin RouteStops innerJoin Routes innerJoin Lines innerJoin Packets)
        .slice(Stops.id)
        .select { Packets.id eq packetId }
        .withDistinct()
        .map { it[Stops.id] }

    println(stopIds.size)
    println(stopIds.groupBy { it }.values.size)

    val nodes = stopIds.map {
        NewNode(
            stopId = it,
            graphId = graphId,
        )
    }.map { node ->
        Node(
            id = (GraphNodes.insert {
                it[GraphNodes.graphId] = node.graphId
                it[GraphNodes.stopId] = node.stopId
            } get GraphNodes.id),
            stopId = node.stopId,
            graphId = node.graphId,
        )
    }

    val routeIds = (Routes innerJoin Lines innerJoin Packets)
        .slice(Routes.id)
        .select { Packets.id eq packetId }
        .map { it[Routes.id] }

    routeIds
        .asSequence()
        .map { selectRouteStopsPairs(it) }
        .flatten()
        .map { pair -> pair.replaceStopIdsForNodeIds(nodes, graphId) }
        .map { pair -> pair.toEdge(graphId) }
        .distinct()
        .toList()
        .forEach { edge ->
            GraphEdges.insert {
                it[GraphEdges.graphId] = edge.graphId
                it[GraphEdges.a] = edge.a
                it[GraphEdges.b] = edge.b
            }
        }

}

private fun Pair<Int, Int>.toEdge(graphId: Int): NewEdge {
    return NewEdge(
        graphId = graphId,
        a = this.first,
        b = this.second,
    )
}

suspend fun main(args: Array<String>) {
    Database.connect("jdbc:postgresql://localhost:5432/TrefuDb?currentSchema=public", driver = "org.postgresql.Driver",
        user = "admin", password = "admin")

    dbQuery {
        Packets
            .selectAll()
            .map { it[Packets.id] }
            .forEach { createGraph(it) }
    }
}