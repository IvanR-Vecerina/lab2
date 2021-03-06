package ch.heig.mac;

import org.neo4j.driver.*;

import java.util.Arrays;
import java.util.List;

public class Main {

    // TODO: Configure credentials to allow connection to your Neo4j instance
    public static Driver openConnection() {
        var uri = "neo4j://localhost:7687";
        var username = "neo4j";
        var password = "";
        var config = Config.defaultConfig();

        return GraphDatabase.driver(uri, AuthTokens.basic(username, password), config);
    }

    public static void main(String[] args) {
        try (var driver = openConnection()) {
            var requests = new Requests(driver);
            requests.getDbLabels().forEach(System.out::println);
            requests.possibleSpreaders().forEach(System.out::println);
            requests.possibleSpreadCounts().forEach(System.out::println);
            requests.sociallyCareful().forEach(System.out::println);
            requests.peopleToInform().forEach(System.out::println);
            requests.setHighRisk().forEach(System.out::println);
            requests.healthyCompanionsOf("Averie Owen").forEach(System.out::println);
            System.out.println(requests.topSickSite());
            requests.sickFrom(Arrays.asList("Kade Acosta", "Eric Robertson", "Gordon Bean", "Rocco Mendez", "Marie Huff", "Averie Owen")).forEach(System.out::println);
        }
    }
}
