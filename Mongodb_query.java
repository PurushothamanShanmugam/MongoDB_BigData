import com.mongodb.client.*;
import com.mongodb.client.model.*;
import org.bson.Document;
import java.io.*;
import java.util.*;

public class Mongodb_query {
    private MongoClient mongoClient;
    private MongoDatabase db;

    public static void main(String[] args) throws Exception {
        Mongodb_query app = new Mongodb_query();
        app.connect();
        // app.load();
        // app.loadNest();

        // System.out.println(app.query1(1000));
        // System.out.println(app.query2(32));
        // System.out.println(app.query2Nest(32));
        // System.out.println("Total Orders: " + app.query3());
        // System.out.println("Total Orders (Nested): " + app.query3Nest());
        // System.out.println(toString(app.query4()));
        System.out.println(toString(app.query4Nest()));
        app.close();
    }

    public void connect() {
        String uri = "mongodb+srv://admin:admin123@cluster1.i1ahzzs.mongodb.net/?retryWrites=true&w=majority&appName=cluster1";
        try {
            mongoClient = MongoClients.create(uri);
            db = mongoClient.getDatabase("tpch");
            System.out.println("Connected to MongoDB Atlas successfully!");
        } catch (Exception e) {
            System.err.println("Failed to connect to MongoDB Atlas");
            e.printStackTrace();
        }
    }

    public void close() {
        if (mongoClient != null) {
            mongoClient.close();
            System.out.println("Connection closed.");
        }
    }

    public void load() throws Exception {
        MongoCollection<Document> customerCol = db.getCollection("customer");
        MongoCollection<Document> ordersCol = db.getCollection("orders");
        customerCol.drop();
        ordersCol.drop();

        try (BufferedReader reader = new BufferedReader(new FileReader("data/customer.tbl"))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\\|");
                Document doc = new Document("c_custkey", Integer.parseInt(parts[0]))
                        .append("c_name", parts[1])
                        .append("c_address", parts[2])
                        .append("c_nationkey", Integer.parseInt(parts[3]))
                        .append("c_phone", parts[4])
                        .append("c_acctbal", Double.parseDouble(parts[5]))
                        .append("c_mktsegment", parts[6])
                        .append("c_comment", parts[7]);
                customerCol.insertOne(doc);
            }
        }

        try (BufferedReader reader = new BufferedReader(new FileReader("data/order.tbl"))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\\|");
                Document doc = new Document("o_orderkey", Integer.parseInt(parts[0]))
                        .append("o_custkey", Integer.parseInt(parts[1]))
                        .append("o_orderstatus", parts[2])
                        .append("o_totalprice", Double.parseDouble(parts[3]))
                        .append("o_orderdate", parts[4])
                        .append("o_orderpriority", parts[5])
                        .append("o_clerk", parts[6])
                        .append("o_shippriority", Integer.parseInt(parts[7]))
                        .append("o_comment", parts[8]);
                ordersCol.insertOne(doc);
            }
        }

        System.out.println("Data loaded into customer and orders collections.");
    }

    public void loadNest() throws Exception {
        MongoCollection<Document> nestedCol = db.getCollection("custorders");
        nestedCol.drop();

        Map<Integer, List<Document>> orderMap = new HashMap<>();

        try (BufferedReader reader = new BufferedReader(new FileReader("data/order.tbl"))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\\|");
                int custKey = Integer.parseInt(parts[1]);
                Document order = new Document("o_orderkey", Integer.parseInt(parts[0]))
                        .append("o_orderstatus", parts[2])
                        .append("o_totalprice", Double.parseDouble(parts[3]))
                        .append("o_orderdate", parts[4])
                        .append("o_orderpriority", parts[5])
                        .append("o_clerk", parts[6])
                        .append("o_shippriority", Integer.parseInt(parts[7]))
                        .append("o_comment", parts[8]);

                orderMap.computeIfAbsent(custKey, k -> new ArrayList<>()).add(order);
            }
        }

        try (BufferedReader reader = new BufferedReader(new FileReader("data/customer.tbl"))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\\|");
                int custKey = Integer.parseInt(parts[0]);
                Document customer = new Document("c_custkey", custKey)
                        .append("c_name", parts[1])
                        .append("c_address", parts[2])
                        .append("c_nationkey", Integer.parseInt(parts[3]))
                        .append("c_phone", parts[4])
                        .append("c_acctbal", Double.parseDouble(parts[5]))
                        .append("c_mktsegment", parts[6])
                        .append("c_comment", parts[7])
                        .append("orders", orderMap.getOrDefault(custKey, new ArrayList<>()));

                nestedCol.insertOne(customer);
            }
        }

        System.out.println("Nested data inserted into 'custorders' collection.");
    }

    public String query1(int custId) {
        Document doc = db.getCollection("customer").find(new Document("c_custkey", custId)).first();
        return (doc != null) ? "Customer Name: " + doc.getString("c_name") : "Customer ID " + custId + " not found.";
    }

    public String query2(int orderId) {
        Document doc = db.getCollection("orders").find(new Document("o_orderkey", orderId)).first();
        return (doc != null) ? "Order Date: " + doc.getString("o_orderdate") : "Order ID " + orderId + " not found.";
    }

    public String query2Nest(int orderId) {
        MongoCollection<Document> nestedCol = db.getCollection("custorders");
        try (MongoCursor<Document> cursor = nestedCol.find().iterator()) {
            while (cursor.hasNext()) {
                List<Document> orders = (List<Document>) cursor.next().get("orders");
                for (Document order : orders) {
                    if (order.getInteger("o_orderkey") == orderId) {
                        return "Order Date (Nested): " + order.getString("o_orderdate");
                    }
                }
            }
        }
        return "Order ID " + orderId + " not found in nested structure.";
    }

    public long query3() {
        return db.getCollection("orders").countDocuments();
    }

    public long query3Nest() {
        long total = 0;
        MongoCollection<Document> nestedCol = db.getCollection("custorders");

        try (MongoCursor<Document> cursor = nestedCol.find().iterator()) {
            while (cursor.hasNext()) {
                List<Document> orders = (List<Document>) cursor.next().get("orders");
                total += (orders != null) ? orders.size() : 0;
            }
        }

        return total;
    }

    public static String toString(Iterator<Document> docs) {
        StringBuilder sb = new StringBuilder("Rows:\n");
        int count = 0;

        while (docs != null && docs.hasNext()) {
            sb.append(docs.next().toJson()).append("\n");
            count++;
        }

        sb.append("Number of rows: ").append(count);
        return sb.toString();
    }

    public Iterator<Document> query4() {
        MongoCollection<Document> ordersCol = db.getCollection("orders");
        MongoCollection<Document> customerCol = db.getCollection("customer");

        AggregateIterable<Document> aggResults = ordersCol.aggregate(Arrays.asList(
                Aggregates.group("$o_custkey", Accumulators.sum("totalSpent", "$o_totalprice")),
                Aggregates.sort(Sorts.descending("totalSpent")),
                Aggregates.limit(5)
        ));

        List<Document> result = new ArrayList<>();
        for (Document groupDoc : aggResults) {
            int custId = groupDoc.getInteger("_id");
            double totalSpent = groupDoc.getDouble("totalSpent");

            Document customer = customerCol.find(new Document("c_custkey", custId)).first();
            String name = (customer != null) ? customer.getString("c_name") : "Unknown";

            result.add(new Document("c_custkey", custId).append("c_name", name).append("totalSpent", totalSpent));
        }

        return result.iterator();
    }

    public Iterator<Document> query4Nest() {
        MongoCollection<Document> nestedCol = db.getCollection("custorders");
        List<Document> result = new ArrayList<>();

        try (MongoCursor<Document> cursor = nestedCol.find().iterator()) {
            while (cursor.hasNext()) {
                Document customer = cursor.next();
                int custKey = customer.getInteger("c_custkey");
                String name = customer.getString("c_name");
                List<Document> orders = (List<Document>) customer.get("orders");

                double total = 0;
                for (Document order : orders) {
                    total += order.getDouble("o_totalprice");
                }

                result.add(new Document("c_custkey", custKey).append("c_name", name).append("totalSpent", total));
            }
        }

        result.sort((a, b) -> Double.compare(b.getDouble("totalSpent"), a.getDouble("totalSpent")));
        return result.stream().limit(5).iterator();
    }
}
