package ch.heig.mac;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.neo4j.driver.*;
import org.neo4j.driver.Record;

public class Requests {
    private static final  Logger LOGGER = Logger.getLogger(Requests.class.getName());
    private final Driver driver;

    public Requests(Driver driver) {
        this.driver = driver;
    }

    public List<String> getDbLabels() {
        var dbVisualizationQuery = "CALL db.labels";

        try (var session = driver.session()) {
            var result = session.run(dbVisualizationQuery);
            return result.list(t -> t.get("label").asString());
        }
    }

    public List<Record> possibleSpreaders() {
        var queue = "MATCH (s:Person {healthstatus: \"Sick\"})-[vs:VISITS]->(:Place)<-[vh:VISITS]-(:Person {healthstatus: \"Healthy\"})\n" +
                "WHERE s.confirmedtime < vs.starttime AND s.confirmedtime < vh.starttime \n" +
                "RETURN DISTINCT s.name AS sickName";

        try (var session = driver.session()) {
            var result = session.run(queue);
            return result.list();
        }
    }

    public List<Record> possibleSpreadCounts() {
        var queue = "MATCH (pSick:Person {healthstatus: \"Sick\"})-[vs:VISITS]-(pl:Place)-[vh:VISITS]-(pHealthy:Person {healthstatus: \"Healthy\"}) \n" +
                "WHERE vh.starttime > vs.starttime AND vs.starttime > pSick.confirmedtime\n" +
                "RETURN pSick.name AS sickName, size(collect(DISTINCT pHealthy)) AS nbHealthy";
        try (var session = driver.session()) {
            var result = session.run(queue);
            return result.list();
        }
    }

    public List<Record> carelessPeople() {
        var queue = "MATCH (s:Person {healthstatus: \"Sick\"})-[vs:VISITS]->(p:Place)\n" +
                "WHERE s.confirmedtime < vs.starttime\n" +
                "WITH s, size(collect(DISTINCT p.name)) AS placeCount \n" +
                "WHERE placeCount > 10\n" +
                "RETURN DISTINCT s.name AS sickName, placeCount AS nbPlaces ORDER BY placeCount DESC";
        try (var session = driver.session()) {
            var result = session.run(queue);
            return result.list();
        }
    }

    public List<Record> sociallyCareful() {
        var queue = "MATCH (pSick:Person {healthstatus: \"Sick\"})\n" +
                "WHERE NOT EXISTS {\n" +
                "    (pSick)-[vs:VISITS]->(:Place {type: \"Bar\"})\n" +
                "    WHERE pSick.confirmedtime < vs.starttime\n" +
                "}\n" +
                "RETURN pSick.name AS sickName";
        try (var session = driver.session()) {
            var result = session.run(queue);
            return result.list();
        }
    }

    public List<Record> peopleToInform() {
        var queue = "MATCH (pSick:Person {healthstatus: \"Sick\"})-[vs:VISITS]->(pl:Place)<-[vh:VISITS]-(pHealthy:Person {healthstatus: \"Healthy\"}) \n" +
                "WHERE duration.between(apoc.coll.max([vs.starttime, vh.starttime]), apoc.coll.min([vs.endtime, vh.endtime])).seconds >= 7200\n" +
                "RETURN pSick.name AS sickName, collect(pHealthy.name) AS peopleToInform";

        try (var session = driver.session()) {
            var result = session.run(queue);
            return result.list();
        }
    }

    public List<Record> setHighRisk() {
        var queue = "MATCH (pSick:Person {healthstatus: \"Sick\"})-[vs:VISITS]->(pl:Place)<-[vh:VISITS]-(pHealthy:Person {healthstatus: \"Healthy\"}) \n" +
                "WHERE duration.between(apoc.coll.max([vs.starttime, vh.starttime]), apoc.coll.min([vs.endtime, vh.endtime])).seconds >= 7200\n" +
                "SET pHealthy.risk = \"high\"\n" +
                "RETURN DISTINCT pHealthy.name AS highRiskName";

        try (var session = driver.session()) {
            var result = session.run(queue);
            return result.list();
        }
    }

    public List<Record> healthyCompanionsOf(String name) {
        Map<String, Object> params = new HashMap<>();
        params.put("input", name);
        var queue = "MATCH (pHealthy:Person {healthstatus: \"Healthy\"})-[:VISITS*..6]-(pSource:Person {name: $input})\n" +
                "RETURN DISTINCT pHealthy.name AS healthyName";
        try (var session = driver.session()) {
            var result = session.run(queue, params);
            return result.list();
        }
    }

    public Record topSickSite() {
        var queue = "MATCH (pSick:Person {healthstatus: \"Sick\"})-[v:VISITS]-(n:Place)\n" +
                "WHERE pSick.confirmedtime <= v.starttime\n" +
                "RETURN n.type AS placeType, size(collect(pSick)) AS nbOfSickVisits\n" +
                "ORDER BY nbOfSickVisits DESC\n" +
                "LIMIT 1";
        try (var session = driver.session()) {
            var result = session.run(queue);
            return result.list().get(0);
        }
    }

    public List<Record> sickFrom(List<String> names) {
        Map<String, Object> params = new HashMap<>();
        params.put("persons", names);
        var queue = "UNWIND $persons AS person\n" +
                "MATCH (pSick:Person {healthstatus: \"Sick\", name: person})\n" +
                "RETURN pSick.name AS sickName";
        try (var session = driver.session()) {
            var result = session.run(queue, params);
            return result.list();
        }
    }
}
