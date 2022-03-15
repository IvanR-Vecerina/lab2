package ch.heig.mac;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
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
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<Record> peopleToInform() {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<Record> setHighRisk() {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<Record> healthyCompanionsOf(String name) {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public Record topSickSite() {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<Record> sickFrom(List<String> names) {
        throw new UnsupportedOperationException("Not implemented, yet");
    }
}
