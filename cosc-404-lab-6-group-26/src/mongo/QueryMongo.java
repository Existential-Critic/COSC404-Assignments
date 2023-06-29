package mongo;

import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.Projections.fields;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Updates;

// Need to import java.util.Arrays in order to cast Array to a list in the load() method
import java.util.Arrays;

// Program to create a collection, insert JSON objects, and perform simple queries on MongoDB.
public class QueryMongo {	
	// MongoDB database name
	public static final String DATABASE_NAME = "lab6";
	
	// MongoDB collection name
	public static final String COLLECTION_NAME = "data";
	
	// MongoDB Server URL
	private static final String SERVER = "localhost";
	
	// Mongo client connection to server
	private MongoClient mongoClient;
	
	// Mongo database
	private MongoDatabase db;
	
	
	/**
	 * Main method
	 * 
	 * @param args
	 * 			no arguments required
	 */	
    public static void main(String [] args) {
    	QueryMongo qmongo = new QueryMongo();
    	qmongo.connect();
    	qmongo.load();
    	System.out.println(QueryMongo.toString(qmongo.query()));
    	qmongo.update(3);
    	System.out.println(QueryMongo.toString(qmongo.query()));
    	System.out.println(QueryMongo.toString(qmongo.query1()));
    	System.out.println(QueryMongo.toString(qmongo.query2()));
	}
        
    /**
     * Connects to Mongo database and returns database object to manipulate for connection.
     *     
     * @return
     * 		Mongo database
     */
    public MongoDatabase connect() {
    	try {
		    // Provide connection information to MongoDB server 
		    mongoClient = MongoClients.create("mongodb://lab6:404mgbpw@"+SERVER);
		}
	    catch (Exception ex) {
			System.out.println("Exception: " + ex);
			ex.printStackTrace();
		}	
		
        // Provide database information to connect to		 
	    // Note: If the database does not already exist, it will be created automatically.
    	db = mongoClient.getDatabase(DATABASE_NAME);
		return db;
    }
    
    // Loads some sample data into MongoDB.
    public void load() {					
		MongoCollection<Document> col;
		// Drop an existing collection (done to make sure you create an empty, new collection each time)
		col = db.getCollection(COLLECTION_NAME);
		if (col != null)
			col.drop();
		
		// See: https://docs.mongodb.com/manual/reference/method/db.createCollection/
		// See: https://docs.mongodb.com/drivers/java/sync/current/fundamentals/databases-collections/		
		MongoCollection<Document> data = db.getCollection(COLLECTION_NAME);

		// See: https://docs.mongodb.com/drivers/java/sync/current/quick-start/
		// See: https://docs.mongodb.com/manual/reference/method/db.collection.insert/
		// See: https://mongodb.github.io/mongo-java-driver/4.4/apidocs/mongodb-driver-core/com/mongodb/BasicDBList.html
		for(int i = 1;i <= 5;i++) {
			// Create the three values document objects
			Document val1Data = new Document().append("val",i).append("text","text"+i);
			Document val2Data = new Document().append("val",i+1).append("text","text"+(i+1));
			Document val3Data = new Document().append("val",i+2).append("text","text"+(i+2));
			// Create a new document containing all required data
			Document newData = new Document().append("key",i).append("name","text"+i).append("num",i).append("values",Arrays.asList(val1Data,val2Data,val3Data));
			data.insertOne(newData);
		}
	}
    
    // Updates a MongoDB record with given key so that the key is 10 times bigger.  The name field should also be updated with the new key value (e.g. text10).
	public void update(int key) {
		// See: https://docs.mongodb.com/drivers/java/sync/current/usage-examples/updateOne/		
		MongoCollection<Document> col = db.getCollection(COLLECTION_NAME);

		// Create the filter document
		Document filterData = new Document().append("key",key);
		// Create the updated document
		Bson updatedData = Updates.combine(
			Updates.set("key", key*10),
			Updates.set("name", "text"+(key*10))
		);
		// Update the row with the filter to the new data
		col.updateMany(filterData,updatedData);
	}
    
    // Performs a MongoDB query that prints out all data (except for the _id).
	public MongoCursor<Document> query() {		
		MongoCollection<Document> col = db.getCollection(COLLECTION_NAME);		
		
		// See: https://docs.mongodb.com/drivers/java/sync/current/usage-examples/find/
		
		MongoCursor<Document> cursor = col.find().projection(fields(include("key", "name", "num", "values"), excludeId())).iterator();
		// OR:
		// MongoCursor<Document> cursor = col.find().projection(excludeId()).iterator();				
		return cursor;				
	}
    
    // Performs a MongoDB query that returns all documents with key < 4.  Only show the key, name, and num fields.
    public MongoCursor<Document> query1() {
    	// Use the code in query() method as a starter.  You must return a cursor to the results.
    	// See: https://docs.mongodb.com/drivers/java/sync/current/usage-examples/find/
		MongoCollection<Document> col = db.getCollection(COLLECTION_NAME);
		
		// Create filter
		Document filterData = new Document("key",new Document("$lt",4));
		// Create cursor to project data
		MongoCursor<Document> cursor = col.find(filterData).projection(fields(include("key","name","num"),excludeId())).iterator();
		// Return the cursor
		return cursor;
    }
    
    // Performs a MongoDB query that returns all documents with key > 2 OR contains an element in the array with val = 4. 
    public MongoCursor<Document> query2() {    	
    	// Use the code in query() method as a starter.  You must return a cursor to the results.
    	// See: https://docs.mongodb.com/manual/reference/operator/query/or/    	
    	// See: https://docs.mongodb.com/manual/reference/operator/query/elemMatch/
    	MongoCollection<Document> col = db.getCollection(COLLECTION_NAME);

		// Create filter
		Document keyClause = new Document("key",new Document("$gt",2));
		Document valClause = new Document("values",new Document("$elemMatch",new Document("val",4)));
		Document filterData = new Document("$or",Arrays.asList(keyClause,valClause));
		// Create cursor to project data
		MongoCursor<Document> cursor = col.find(filterData).iterator();
		// Return the cursor
		return cursor;
    }          
    
    /**
     * Returns the Mongo database being used.
     * 
     * @return
     * 		Mongo database
     */
    public MongoDatabase getDb() {
    	return db;    
    }
    
    /**
     * Outputs a cursor of MongoDB results in string form.
     * 
     * @param cursor
     * 		Mongo cursor
     * @return
     * 		results as a string
     */
    public static String toString(MongoCursor<Document> cursor) {
    	StringBuilder buf = new StringBuilder();
    	int count = 0;
    	buf.append("Rows:\n");
    	if (cursor != null) {	    	
			while (cursor.hasNext()) {
				Document obj = cursor.next();
				buf.append(obj.toJson());
				buf.append("\n");
				count++;
			}
    	}
		buf.append("Number of rows: "+count);
		cursor.close();
		return buf.toString();
    }
} 