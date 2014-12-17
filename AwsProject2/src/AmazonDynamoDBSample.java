/*
 * -----------------------------------------------------
 * Source: Amazon DynamoDB Developer guide, API guide.
 * Demo of DynamoDB provided all Low Level API actions
 * and how to do operations.
 * 
 * By: Ahsanul Hadi 
 * Email: ahsanulhadi.dsi@gmail.com, adil.gt@gmail.com
 * -----------------------------------------------------
 * Last update date: 11-DEC-2014
 * Updated: 
 * -----------------------------------------------------
 * WANRNING:
 * To avoid accidental leakage of your credentials, DO NOT keep the credentials file in your source directory.
 */

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Arrays;
import java.util.Map.Entry;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
//import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.util.Tables;
import com.amazonaws.services.dynamodbv2.model.*;

/*
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableResult;
//import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ListTablesRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.Select;
import com.amazonaws.services.dynamodbv2.model.UpdateTableRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateTableResult;
//import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;

*/

public class AmazonDynamoDBSample {

    static AmazonDynamoDBClient dynamoDB;
    static String tableName = "ProductOrder";
	static String[] ProductId_list= new String[]{"M001","M002","P001"}; 

	static String PaymentHistory = "[" 			   
			+   "{"
			+   "\"PaymentDate\" :\"2014.11.15\" ,"
			+   "\"Amount\" : 3200 ,"
			+   "\"PaymentMethod\" : \"Cash\""
			+   "},"
			+   "{"
			+   "\"PaymentDate\" :\"2014.11.20\" ,"
			+   "\"Amount\" : 28000 ,"
			+   "\"PaymentMethod\" : \"Cheque\" ,"
			+   "\"ChequeNo\" : \"SB1435-0786-123\" ,"
			+   "\"BankName\" : \"Dutch Bangla Bank\""
			+   "}"						
			+ "],";

	static String CompanyContacts ="[" 			   
			+   "{"
			+   "\"Name\" :\"Rafik\" ,"
			+   "\"Designation\" :\"Sales Manager\" ,"
			+   "\"ContactNo\" : \"01912433566\""
			+   "},"
			+   "{"
			+   "\"Name\" :\"Karim\" ,"
			+   "\"Designation\" :\"Sales Executive\" ,"
			+   "\"ContactNo\" : \"01912433567\""
			+   "}"
			+ "],";

	static String CompanyAddress = "{"
			+   "\"Street\": \"12/A, xyz road, ABC area ...\","
			+   "\"City\":\"Dhaka\","
			+   "\"Country\": \"Bangladesh\""
			+   "},";
	
    /*
     * The only information needed to create a client are security credentials
     * consisting of the AWS Access Key ID and Secret Access Key. All other
     * configuration, such as the service endpoints, are performed
     * automatically. Client parameters, such as proxies, can be specified in an
     * optional ClientConfiguration object when constructing a client.
     *
     * @see com.amazonaws.auth.BasicAWSCredentials
     * @see com.amazonaws.auth.ProfilesConfigFile
     * @see com.amazonaws.ClientConfiguration
     */
    
    // ======== Main Method ==============================================
    public static void main(String[] args) throws Exception 
    {
        init();
        /* ------- Example: MANAGE Table ------- */ 
        createTable(tableName); 		// Working OK. 
        //describeTable(tableName);   	// Working OK.
        //updateTable(tableName);		// Working OK.
        //listTable();     				// Working OK.
        //deleteTable(tableName);		// Working OK.
        
        /*  -------- CRUD Operations ------------ */ 
        
        /*  -------  Example: MODIFY Data  ------- */
        //addItems(tableName);   					// Working OK. 
        //ConditionalUpdateItems(tableName, null); 	// Working OK. 
        //BatchWriteItems(tableName); 				// Working OK. 
        
        
        /*  -------  Example: READ Data  -------  */
        //getItem(tableName, "101", "20141201090909");   // getItem(String tableName, String Id, String Orderdate)
        //scanItems(tableName);    		// Working OK.    
        
        // Values for queryItem Index: index_CompanyName, index_DeliveryDate, null
        queryItems(tableName, "index_DeliveryDate"); // Working OK. 
        queryItems(tableName, "index_CompanyName");  // Working OK.
        queryItems(tableName, null); 				// Working OK.
        
    }

    // ======== Initialize DynamoDB Client with proper credentials =======
    private static void init() throws Exception 
    {
        /*
         * The ProfileCredentialsProvider will return your [default]
         * credential profile by reading from the credentials file located at
         * (C:\\Users\\<User Name>\\.aws\\credentials).
         */
    	
    	/* -- Another way to get credentials ----- 
    	 * AWSCredentials credentials = new PropertiesCredentials(
         *      AmazonDynamoDBSample.class.getResourceAsStream("AwsCredentials.properties"));
         *      dynamoDB = new AmazonDynamoDBClient(credentials);
    	 */
        AWSCredentials credentials = null;
        try 
        {
            
        	credentials = new ProfileCredentialsProvider("TestUser").getCredentials();
            try
            {
            	// Create an instance of the AmazonDynamoDBClient class.
            	dynamoDB = new AmazonDynamoDBClient(credentials);
            	/* Set Region = ap-southeast-1   | for Asia Pacific (Singapore) 
            	 * host: dynamodb.ap-southeast-1.amazonaws.com	| allowed: HTTP and HTTPS           
            	 */
            	Region apSouthEast1 = Region.getRegion(Regions.AP_SOUTHEAST_1);
            	dynamoDB.setRegion(apSouthEast1);
            }
            catch (AmazonServiceException ase) 
            {
            	printServiceExceptionError(ase);
            } 

            catch (AmazonClientException ace) 
            {
            	printClientExceptionError(ace);
            }
        } 
        catch (Exception e) 
        {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (C:\\Users\\<User name>\\.aws\\credentials), and is in valid format.",
                    e);
        }
    }

    // ==================================================================================
    // Section:  MANAGING TABLE - DynamoDB Low Level APIs
    // Actions: Create / Describe / Update / List / Delete table. 
    // ==================================================================================

    // ======== Create a Table ===========================================    
	private static void createTable(String tableName) throws Exception 
    {
		/* 
		 * Index issues: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/SecondaryIndexes.html
		 */
		
		String hashItemName = "OrderId"; // will be used as HASH Key
		String rangeItemName = "OrderDate";  // Will be used as a RANGE Key
		
		String LSIitem1 = "CompanyName";  // will be used as Local Secondary Index
		String LSIname1 = "index_CompanyName"; // Local Secondary Index name.
		String LSIitem1_ProjKey1 = "DeliveryDate"; // will be Projected for Local Secondary index
		String LSIitem1_ProjKey2 = "DeliveryStatus"; // will be Projected for Local Secondary index
		
		String LSIitem2 = "DeliveryDate"; // will be used as Local Secondary Index
		String LSIname2 = "index_DeliveryDate"; // Local Secondary Index name. will be projected on ALL 

		/*
		String LSIitem3 = "DeliveryStatus"; // will be used as Local Secondary Index
		String LSIname3 = "index_DeliveryStatus"; // Local Secondary Index name.
		String LSIitem3_ProjKey1 = "CompanyName"; // will be Projected for Local Secondary index
		String LSIitem3_ProjKey2 = "CompanyAddress"; // will be Projected for Local Secondary index
		String LSIitem3_ProjKey3 = "CompanyContacts"; // will be Projected for Local Secondary index
		*/
		
		
		long RCU = 1L; // RCU = Read Capacity Units  
		long WCU = 1L; // WCU = Write Capacity Units
		
		
		System.out.println("---------------------------------------------");
		System.out.println("CREATE TABLE: \n");

        if (Tables.doesTableExist(dynamoDB, tableName)) 
        {
            System.out.println("- (X) Table " + tableName + " is already ACTIVE");
        }
        else
        {
        	try
        	{
            	/* ------------------ CREATE TABLE Section: Start --------------------------------------- */
        		CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName);
    			DescribeTableRequest describeTableRequest = new DescribeTableRequest().withTableName(tableName);

        		// Provisioned Throughput
        		createTableRequest.setProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(RCU).withWriteCapacityUnits(WCU));
        		
        		// Attribute Definitions 
        		ArrayList<AttributeDefinition> attributeDefinitions= new ArrayList<AttributeDefinition>();
        		attributeDefinitions.add(new AttributeDefinition().withAttributeName(hashItemName).withAttributeType("N"));  // Hash Key
        		attributeDefinitions.add(new AttributeDefinition().withAttributeName(rangeItemName).withAttributeType("N")); // Range Key	
        		attributeDefinitions.add(new AttributeDefinition().withAttributeName(LSIitem1).withAttributeType("S")); // Local Secondary Index 1
        		attributeDefinitions.add(new AttributeDefinition().withAttributeName(LSIitem2).withAttributeType("N")); // Local Secondary Index 2

        		// Can not create Projected key Attributes now. Will not be allowed without Values.
        		
        		createTableRequest.setAttributeDefinitions(attributeDefinitions);
        				        
        		// Key Schema for Hash & Range Key 		
        		ArrayList<KeySchemaElement> tableKeySchema = new ArrayList<KeySchemaElement>();
        		tableKeySchema.add(new KeySchemaElement().withAttributeName(hashItemName).withKeyType(KeyType.HASH));
        		tableKeySchema.add(new KeySchemaElement().withAttributeName(rangeItemName).withKeyType(KeyType.RANGE));

        		createTableRequest.setKeySchema(tableKeySchema);

        		// ----- INDEX #1: Local Secondary Index (LSI) on Company Name    
        		ArrayList<LocalSecondaryIndex> localSecondaryIndexes = new ArrayList<LocalSecondaryIndex>();		
        		
        		LocalSecondaryIndex LSindex1 = new LocalSecondaryIndex().withIndexName(LSIname1);
        		
        		// Key Schema for LSindex i.e. Local Secondary Index #1 
        		ArrayList<KeySchemaElement> IndexKeySchema = new ArrayList<KeySchemaElement>();
        		
        		IndexKeySchema.add(new KeySchemaElement().withAttributeName(hashItemName).withKeyType(KeyType.HASH));
        		IndexKeySchema.add(new KeySchemaElement().withAttributeName(LSIitem1).withKeyType(KeyType.RANGE));
        		
        		LSindex1.setKeySchema(IndexKeySchema);
        		
        		// Projected Attributes for LSindex  i.e. Local Secondary Index.
        		Projection projection = new Projection().withProjectionType(ProjectionType.INCLUDE);
        		ArrayList<String> nonKeyAttributes = new ArrayList<String>();
        		nonKeyAttributes.add(LSIitem1_ProjKey1);
        		nonKeyAttributes.add(LSIitem1_ProjKey2);

        		projection.setNonKeyAttributes(nonKeyAttributes);
        		LSindex1.setProjection(projection);

        		localSecondaryIndexes.add(LSindex1);
        		
        		// ----- INDEX #2: Local Secondary Index (LSI) on Delivery Date        		
        		LocalSecondaryIndex LSindex2 = new LocalSecondaryIndex().withIndexName(LSIname2);
        		// Key Schema for LSindex i.e. Local Secondary Index # 2
        		IndexKeySchema = new ArrayList<KeySchemaElement>();
        		IndexKeySchema.add(new KeySchemaElement().withAttributeName(hashItemName).withKeyType(KeyType.HASH));
        		IndexKeySchema.add(new KeySchemaElement().withAttributeName(LSIitem2).withKeyType(KeyType.RANGE));
        		
        		LSindex2.setKeySchema(IndexKeySchema);
        		
        		// Projected Attributes for LSindex  i.e. Local Secondary Index.
        		projection = new Projection().withProjectionType(ProjectionType.ALL);        		
        		LSindex2.setProjection(projection);
        		
        		localSecondaryIndexes.add(LSindex2);
        		
        		// --------
        		createTableRequest.setLocalSecondaryIndexes(localSecondaryIndexes);

        		CreateTableResult result = dynamoDB.createTable(createTableRequest);

        		/* ------------------ CREATE TABLE Section: End --------------------------------------- */
        		// Get current status of that Table
        		System.out.println("- Current Table Status: " + result.getTableDescription().getTableStatus());
        		// Get Full Table Description 
        		// System.out.println(result.getTableDescription());

                /* 
                 // Another way to get Table description:  
                 TableDescription createdTableDescription = dynamoDB.createTable(createTableRequest).getTableDescription();
                 System.out.println("Created Table Properties: " + createdTableDescription);
                 System.out.println("Status of the Table: " + createdTableDescription.getTableStatus());
                 */

                // Wait for Table to become ACTIVE: (Call method of Tables)
                System.out.println("- Waiting for table [" + tableName + "] to become ACTIVE ...");                
                Tables.waitForTableToBecomeActive(dynamoDB, tableName);
                
    	        TableDescription tableDescription = dynamoDB.describeTable(describeTableRequest).getTable(); 
                System.out.println("- Current Table Status: " + tableDescription.getTableStatus());
        	}
            catch (AmazonServiceException ase) 
            {
            	printServiceExceptionError(ase);
            } 
            catch (AmazonClientException ace) 
            {
            	printClientExceptionError(ace);
            }
        } //End of ELSE part.
	}

	// ======== Describe a Table =========================================
	private static void describeTable(String tableName) throws Exception 
    {
		System.out.println("\n---------------------------------------------");
		System.out.println("DESCRIBE TABLE: \n");
		
    	try
    	{
            if (Tables.doesTableExist(dynamoDB, tableName))             	
            {
                DescribeTableRequest describeTableRequest = new DescribeTableRequest().withTableName(tableName);
                TableDescription tableDescription = dynamoDB.describeTable(describeTableRequest).getTable();  
                
            	// Describe our new table                      
                System.out.println("Table Description:");
                System.out.println(tableDescription);

                /*
                System.out.printf("%s: %s \t ReadCapacityUnits: %d \t WriteCapacityUnits: %d",
                		  tableDescription.getTableStatus(),
                		  tableDescription.getTableName(),
                		  tableDescription.getProvisionedThroughput().getReadCapacityUnits(),
                		  tableDescription.getProvisionedThroughput().getWriteCapacityUnits());
                */
            	// Describe INDEX. This code snippet will work for multiple indexes.
            	List<LocalSecondaryIndexDescription> localSecondaryIndexes = tableDescription.getLocalSecondaryIndexes();
            	            	
            	Iterator<LocalSecondaryIndexDescription> lsiIter = localSecondaryIndexes.iterator();
            	while (lsiIter.hasNext()) 
            	{
            		LocalSecondaryIndexDescription lsiDescription = lsiIter.next();
            		System.out.println("Info for index '" + lsiDescription.getIndexName() + "':");
            		
            		Iterator<KeySchemaElement> kseIter = lsiDescription.getKeySchema().iterator();
            		while (kseIter.hasNext()) 
            		{
            			KeySchemaElement kse = kseIter.next();
            			System.out.printf("\t%s: %s\n", kse.getAttributeName(), kse.getKeyType());
            		}
            		
            	    Projection projection = lsiDescription.getProjection();
            	    System.out.println("\tThe projection type is: " + projection.getProjectionType());
            	    if (projection.getProjectionType().toString().equals("INCLUDE")) 
            	    {
            	    	System.out.println("\t\tThe non-key projected attributes are: " + projection.getNonKeyAttributes());
            	    }
            	} // End While
            } // End IF
            else 
            {
            	System.out.println("(X) " + tableName + " does not exist !");
            }
     	}
        catch (AmazonServiceException ase) 
        {
        	printServiceExceptionError(ase);
        } 
        catch (AmazonClientException ace) 
        {
        	printClientExceptionError(ace);
        }
	}

	// ======== Update a Table ===========================================
	private static void updateTable(String tableName) throws Exception
	{
		/*
		 * You can update only the provisioned throughput values of an existing table. Depending on you application requirements, 
		 * you might need to update these values. You can increase the read capacity units and write capacity units anytime. 
		 * However, you can decrease these values only four times in a 24 hour period. 
		 * For additional guidelines and limitations, see Specifying Read and Write Requirements for Tables.
		 * http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithTables.html#ProvisionedThroughput
		 */ 
		long RCU = 2L; // RCU = Read Capacity Units  
		long WCU = 2L; // WCU = Write Capacity Units
		long startTime = System.currentTimeMillis();
		
		String tableStatusNow = null;
		String statusActive = "ACTIVE";
		//String tableStatusPrev = null;
		
		try
		{
			DescribeTableRequest describeTableRequest = new DescribeTableRequest().withTableName(tableName);
	        TableDescription tableDescription = dynamoDB.describeTable(describeTableRequest).getTable(); 
	        tableStatusNow = tableDescription.getTableStatus();
	        
			System.out.println("\n---------------------------------------------");
			System.out.println("UPDATE TABLE: Modify Provisioned Throughput -\n");
			
			System.out.println("- Current RCU:" + tableDescription.getProvisionedThroughput().getReadCapacityUnits());
			System.out.println("- Current WCU:" + tableDescription.getProvisionedThroughput().getWriteCapacityUnits());
			
			System.out.println("- Current Table Status: " + tableStatusNow);
			
			if (!tableStatusNow.equals(statusActive))
			{
				System.out.println("(X) " + tableName + " is not Active yet. Try to update few minutes later.");
			}
			else
			{
				/* Can be done this was as well. 
				ProvisionedThroughput provisionedThroughput = new ProvisionedThroughput().withReadCapacityUnits(RCU).withWriteCapacityUnits(WCU);
				UpdateTableRequest updateTableRequest = new UpdateTableRequest().withTableName(tableName).withProvisionedThroughput(provisionedThroughput);
				*/
				
				UpdateTableRequest updateTableRequest = new UpdateTableRequest()
										.withTableName(tableName)
										.withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(RCU).withWriteCapacityUnits(WCU));
				
				UpdateTableResult result = dynamoDB.updateTable(updateTableRequest);
						
				System.out.println("- Table's Provisioned Throughput is updated.");
				System.out.println("- Current Table Status: " + result.getTableDescription().getTableStatus());
		        System.out.println("- Waiting for table [" + tableName + "] to become ACTIVE ...");                
		        
		        // Tables.waitForTableToBecomeActive(dynamoDB, tableName);
		        do 
		        {
		        	Tables.waitForTableToBecomeActive(dynamoDB, tableName);
		        	tableDescription = dynamoDB.describeTable(describeTableRequest).getTable();
		        	tableStatusNow = tableDescription.getTableStatus();        	            
		        } while (!tableStatusNow.equals(statusActive));
		        
		        long stopTime = System.currentTimeMillis();
		        long elapsedTime = stopTime - startTime;
		        		        
		        System.out.println("- Current Table Status: " + tableStatusNow);
				System.out.println("- Current RCU:" + tableDescription.getProvisionedThroughput().getReadCapacityUnits());
				System.out.println("- Current WCU:" + tableDescription.getProvisionedThroughput().getWriteCapacityUnits());
		        System.out.println("- Times Taken: " + (elapsedTime/1000) + " sec.");
		        
			}	
		}
        catch (AmazonServiceException ase) 
        {
        	printServiceExceptionError(ase);
        } 
        catch (AmazonClientException ace) 
        {
        	printClientExceptionError(ace);
        }
	}

	// ======== List Table ===============================================
	private static void listTable() throws Exception
	{
		/*
		 * The ListTables operation requires no parameters. However, you can specify optional parameters. 
		 * For example, you can set the limit parameter if you want to use paging to limit the number of table names per page. 
		 * First, create a ListTablesRequest object and provide optional parameters. 
		 * Along with the page size, the request sets the exclusiveStartTableName parameter. 
		 * Initially, exclusiveStartTableName is null, however, after fetching the first page of result, 
		 * to retrieve the next page of result, you must set this parameter value to the lastEvaluatedTableName 
		 * property of the current result.
		 */
		// Initial value for the first page of table names.
		
		String lastEvaluatedTableName = null;
		int counter = 1;
		do 
		{
		    
		    ListTablesRequest listTablesRequest = new ListTablesRequest().withLimit(10).withExclusiveStartTableName(lastEvaluatedTableName);
		    
		    ListTablesResult result = dynamoDB.listTables(listTablesRequest);
		    lastEvaluatedTableName = result.getLastEvaluatedTableName();
		    
		    
		    System.out.println("\n---------------------------------------------");
			System.out.println("LIST TABLE: \n");
		    for (String name : result.getTableNames()) 
		    {
		        System.out.println("(" + counter + ") " + name);
		        counter ++; 
		    }
		    
		} while (lastEvaluatedTableName != null);
		
	}

	// ======== Delete Table ===============================================
	private static void deleteTable(String tableName) throws Exception
	{
		System.out.println("\n---------------------------------------------");
		System.out.println("DELETE TABLE: " + tableName + "\n");

		try
		{
			DeleteTableRequest deleteTableRequest = new DeleteTableRequest().withTableName(tableName);
			DeleteTableResult result = dynamoDB.deleteTable(deleteTableRequest);
			System.out.println("- Current Table Status: " + result.getTableDescription().getTableStatus());
		}
        catch (AmazonServiceException ase) 
        {
        	printServiceExceptionError(ase);
        } 
        catch (AmazonClientException ace) 
        {
        	printClientExceptionError(ace);
        }		
	}


    // ==================================================================================
    // Section:  MODIFYING DATA 
    // Actions: Put Item / Update Item / Delete Item / Batch Write Item 
    // ==================================================================================
    
	/* 
	 * You can use AWS SDK for Java low-level API (protocol-level API) to perform typical create, read, update, and delete (CRUD) operations 
	 * on an item in a table. The Java API for item operations maps to the underlying DynamoDB API. For more information, see Using the DynamoDB API.
	 * Note that the AWS SDK for Java also provides a high-level object persistence model, enabling you to map your client-side classes to 
	 * DynamoDB tables. This approach can reduce the amount of code you have to write. For more information, see Java: Object Persistence Model.
	 * More: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/LowLevelJavaItemCRUD.html
	 */
	// ======== Put item / Add item ============================================
    private static void addItems(String tableName) throws Exception
    {
    	/*
    	 * Creates a new item. If an item with the same key already exists in the table, it is replaced with the new item. 
    	 */
    			
    	PutItemRequest itemRequest;
    	PutItemResult putItemResult;
    	// GetCurrentDateTime()    
    	System.out.println("\n---------------------------------------------");
    	System.out.println("PUT/ADD ITEM: \n");
    
    	try 
        {
			Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();       	
			// Add item: Way #2
			//String[] CourseId = new String[]{"CSE101","CSE102","MGT101"};
			// someFunction(AuthorList);
			
			/*
			newItem(int OrderId, int OrderDate, String[] ProductId_list, String DeliveryDate, String DeliveryStatus, 
		    		String DeliveredBy, int DeliveryCost, int DiscountAmount, String TotalCost, String json_PaymentHistory,
		    		int Due, String CompanyName, String json_CompanyContacts, String json_CompanyAddress, boolean Flagged) 
		    */
			// Sample item #1
			// Sample status = Delivered, Pending, Halt, Returned, Missing ....
			
			
			// int orderDate = Integer.parseInt(GetCurrentDateTime());
			long orderDate = 20141201090909L;
			String DeliveryDate = "20141125";
			
        	item = newItem(101, orderDate, ProductId_list, DeliveryDate, "Pending",
        			"S.A. Paribahan", 20, 0, "1000", PaymentHistory,
        			200, "ABC Stationary Shop", CompanyContacts, CompanyAddress, false);
        	
            itemRequest = new PutItemRequest(tableName, item);
            putItemResult= dynamoDB.putItem(itemRequest);
            System.out.println("Add item Result: " + putItemResult);
            item.clear();                     	
        } 
        catch (AmazonServiceException ase) 
        {
        	printServiceExceptionError(ase);
        } 
        catch (AmazonClientException ace) 
        {
        	printClientExceptionError(ace);            
        }
    	
    }

	// ======== Conditional Put item / Add item ============================================
    private static void ConditionalUpdateItems(String tableName, String condition) throws Exception
    {
    	/*
    	 * Use an optional parameter to specify a condition for uploading the item. 
    	 * If the condition specified is not met, then the AWS Java SDK throws a ConditionalCheckFailedException. 
    	 * http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/LowLevelJavaItemCRUD.html
    	 * Example: Here we will check - 
    	 * IF we want to update DeliveryStatus = Delivered 
    	 * THEN for that record (orderID) Due cannot be > 0 . IF Due > 0 then don't allow the update.  
    	 */
    	
    	PutItemRequest itemRequest;
    	PutItemResult putItemResult;
    	// GetCurrentDateTime()
    
    	System.out.println("\n---------------------------------------------");
    	System.out.println("PUT ITEM with Condition: \n");
    
    	try 
        {
			Map<String, AttributeValue> item = new HashMap<String, AttributeValue>(); // for Adding item. 
			
			long orderDate = 20141201090909L;   // 20141102102003
			/*
			newItem(int OrderId, int OrderDate, String json_ProductList, String DeliveryDate, String DeliveryStatus, 
		    		String DeliveredBy, int DeliveryCost, int DiscountAmount, String TotalCost, String json_PaymentHistory,
		    		int Due, String CompanyName, String json_CompanyContacts, String json_CompanyAddress, boolean Flagged) 
		    */
        	item = newItem(101, orderDate, ProductId_list, GetCurrentDateTime(), "Delivered",
        			"S.A. Paribahan", 100, 0, "1000", PaymentHistory,
        			0, "XYZ Stationary Shop", CompanyContacts, CompanyAddress, false);
        	
			Map<String, AttributeValue> expressionAttributeValues = new HashMap<String, AttributeValue>(); // for condition expression.
			// expressionAttributeValues.put(":val", new AttributeValue().withS("Delivered")); // for Delivery Status.  
			expressionAttributeValues.put(":val", new AttributeValue().withN("0")); // for Due

            itemRequest = new PutItemRequest().withTableName(tableName).withItem(item)
            															.withConditionExpression("Due = :val")
            															.withExpressionAttributeValues(expressionAttributeValues)
            															.withReturnValues(ReturnValue.ALL_OLD);
            putItemResult= dynamoDB.putItem(itemRequest);
            System.out.println("Add item Result: " + putItemResult);
            item.clear();                     	
        } 
        catch (AmazonServiceException ase) 
        {
        	printServiceExceptionError(ase);
        } 
        catch (AmazonClientException ace) 
        {
        	printClientExceptionError(ace);            
        }
    	
    }

	// ======== BATCH Write: Put multiple item & Delete them ============================================
    private static void BatchWriteItems(String tableName) throws Exception
    {
    	
    	/*
    	 * You should check if there were any unprocessed request items returned in the response. This could happen if you reach the 
    	 * provisioned throughput limit or some other transient error. Also, DynamoDB limits the request size and the number of operations 
    	 * you can specify in a request. If you exceed these limits, DynamoDB rejects the request. 
    	 * http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/batch-operation-lowlevel-java.html#LowLevelJavaBatchWrite
    	 * 
    	 * If DynamoDB returns any unprocessed items, you should retry the batch operation on those items. However, we strongly 
    	 * recommend that you use an exponential backoff algorithm. If you retry the batch operation immediately, the underlying read or write 
    	 * requests can still fail due to throttling on the individual tables. If you delay the batch operation using exponential backoff, 
    	 * the individual requests in the batch are much more likely to succeed.
    	 * 
    	 * http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ErrorHandling.html#APIRetries
    	 * http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ErrorHandling.html#BatchOperations
    	 */
    	
    	/*
    		If one or more of the following is true, DynamoDB rejects the entire batch write operation:
    		- One or more tables specified in the BatchWriteItem request does not exist.
    		- Primary key attributes specified on an item in the request do not match those in the corresponding table's primary key schema.
    		- You try to perform multiple operations on the same item in the same BatchWriteItem request. 
    		For example, you cannot put and delete the same item in the same BatchWriteItem request.

    		- There are more than 25 requests in the batch.
    		- Any individual item in a batch exceeds 400 KB.
    		- The total request size exceeds 16 MB.
    	*/
    	
		long orderDate;
		String DeliveryDate;
		String hashItemName = "OrderId"; // will be used as HASH Key
		String rangeItemName = "OrderDate";  // Will be used as a RANGE Key
    	System.out.println("\n---------------------------------------------");
    	System.out.println("BATCH Write Operations: \n");
		
    	try
    	{
    		// Create a map for the requests in the batch. Table Name and job List.
    		Map<String, List<WriteRequest>> requestItems = new HashMap<String, List<WriteRequest>>();
    		// Create an Array List to add Tasks in the Job List. 
    		List<WriteRequest> jobList = new ArrayList<WriteRequest>();
    		        
    		// Create a PutRequest for a new Forum item
    		//Map<String, AttributeValue> forumItem = new HashMap<String, AttributeValue>();
    		Map<String, AttributeValue> item = new HashMap<String, AttributeValue>(); // for Adding item. 
    		
    		/*
    		forumItem.put("Name", new AttributeValue().withS("Amazon RDS"));
    		forumItem.put("Threads", new AttributeValue().withN("0"));
    		*/
			/*
			newItem(int OrderId, int OrderDate, String[] ProductId_list, String DeliveryDate, String DeliveryStatus, 
		    		String DeliveredBy, int DeliveryCost, int DiscountAmount, String TotalCost, String json_PaymentHistory,
		    		int Due, String CompanyName, String json_CompanyContacts, String json_CompanyAddress, boolean Flagged) 
		    */
    		orderDate = 20141201101010L;
    		DeliveryDate = "20141220";
    		
        	item = newItem(201, orderDate, ProductId_list, DeliveryDate, "Pending",
        			"S.A. Paribahan", 20, 0, "1000", PaymentHistory,
        			0, "ABC Stationary Shop", CompanyContacts, CompanyAddress, false);
    		jobList.add(new WriteRequest().withPutRequest(new PutRequest().withItem(item)));
    		
    		orderDate = 20141201111010L;
    		DeliveryDate = "20141225";
    		
        	item = newItem(202, orderDate, ProductId_list, DeliveryDate, "Pending",
        			"S.A. Paribahan", 100, 0, "5000", PaymentHistory,
        			0, "XYZ Stationary Shop", CompanyContacts, CompanyAddress, false);
    		jobList.add(new WriteRequest().withPutRequest(new PutRequest().withItem(item)));
    		
    		//requestItems.put(tableName, jobList);
    		// We can add item to a different table in the same way. 
    		
    		// Create a DeleteRequest 
    		HashMap<String, AttributeValue> DeleteKey = new HashMap<String, AttributeValue>();
    		DeleteKey.put(hashItemName, new AttributeValue().withN("102"));
    		DeleteKey.put(rangeItemName, new AttributeValue().withN("20141201090909"));
    		    
    		jobList.add(new WriteRequest().withDeleteRequest(new DeleteRequest().withKey(DeleteKey)));
    		
    		// Finally add the list as a RequestItem.
    		requestItems.put(tableName, jobList);
            
            BatchWriteItemResult result;
            BatchWriteItemRequest batchWriteItemRequest = new BatchWriteItemRequest().withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
            
            do 
            {
                System.out.println("Making the batch request.");
                                
                batchWriteItemRequest.withRequestItems(requestItems);
                result = dynamoDB.batchWriteItem(batchWriteItemRequest);
                
                // Print consumed capacity units
                for(ConsumedCapacity consumedCapacity : result.getConsumedCapacity()) 
                {
                     String ThisTableName = consumedCapacity.getTableName();   // in case of multiple table operations
                     Double consumedCapacityUnits = consumedCapacity.getCapacityUnits();
                     System.out.println("- Consumed capacity units for table " + ThisTableName + ": " + consumedCapacityUnits);
                }
                
                // Check for unprocessed keys which could happen if you exceed provisioned throughput
                System.out.println("Unprocessed Put and Delete requests: \n" + result.getUnprocessedItems());
                requestItems = result.getUnprocessedItems();
            } while (result.getUnprocessedItems().size() > 0); 		
    	}
    	catch (AmazonServiceException ase) 
    	{
    		printServiceExceptionError(ase);
    	} 
    	catch (AmazonClientException ace) 
    	{
    		printClientExceptionError(ace);            
    	}
    	
    }
    
    // ======== Method for adding items in Hash map ================================     
    private static Map<String, AttributeValue> newItem(int OrderId, long OrderDate, String[] ProductId_list, 
    		String DeliveryDate, String DeliveryStatus, String DeliveredBy, int DeliveryCost, int DiscountAmount, 
    		String TotalCost, String PaymentHistory, int Due, String CompanyName, String CompanyContacts, 
    		String CompanyAddress, boolean Flagged)
    {
		// private static Map<String, AttributeValue> newItem(String name, int year, String rating, String... fans)
        // private static Map<String, AttributeValue> newItem(String StudentName, String RegDateTime, String[] CourseId)
    	
    	Map<String, AttributeValue> item1 = new HashMap<String, AttributeValue>();
		// Add the items
    	item1.put("OrderId", new AttributeValue().withN(Integer.toString(OrderId)));
    	item1.put("OrderDate", new AttributeValue().withN(Long.toString(OrderDate)));
    	//item1.put("OrderDate", new AttributeValue(OrderDate));
    	item1.put("ProductId_list", new AttributeValue().withSS(ProductId_list));
    	//item.put("Authors", new AttributeValue().withSS(Arrays.asList("Author1", "Author2"))); 	
    	item1.put("DeliveryDate", new AttributeValue().withN(DeliveryDate));
    	item1.put("DeliveryStatus", new AttributeValue(DeliveryStatus));
		item1.put("DeliveredBy", new AttributeValue().withS(DeliveredBy));			
		item1.put("DeliveryCost", new AttributeValue().withN(Integer.toString(DeliveryCost)));
		item1.put("DiscountAmount", new AttributeValue().withN(Integer.toString(DiscountAmount)));
		item1.put("TotalCost", new AttributeValue().withN(String.valueOf(TotalCost)));
		item1.put("PaymentHistory", new AttributeValue().withS(PaymentHistory));
		item1.put("Due", new AttributeValue().withN(String.valueOf(Due)));		
		item1.put("CompanyName", new AttributeValue().withS(CompanyName));		
		item1.put("CompanyContacts", new AttributeValue().withS(CompanyContacts));		
		item1.put("CompanyAddress", new AttributeValue().withS(CompanyAddress));
		item1.put("Flagged", new AttributeValue().withBOOL(Flagged));
        //item.put("fans", new AttributeValue().withSS(fans));
		//item.put("Authors", new AttributeValue().withSS(Arrays.asList("Author1", "Author2")));
        return item1;
    }

	
    // ==================================================================================
    // Section:  READING DATA 
    // Actions: Get Item / Batch Get Item / Query / Scan 
    // ==================================================================================

    /* < TIPS >
     * Generally, a Query operation is more efficient than a Scan operation. 
     * A Scan operation always scans the entire table, then filters out values to provide the desired result, 
     * essentially adding the extra step of removing data from the result set. Avoid using a Scan operation on a large table with a 
     * filter that removes many results, if possible. Also, as a table grows, the Scan operation slows. The Scan operation examines 
     * every item for the requested values, and can use up the provisioned throughput for a large table in a single operation. 
     * For quicker response times, design your tables in a way that can use the Query, Get, or BatchGetItem APIs, instead. 
     * Alternatively, design your application to use Scan operations in a way that minimizes the impact on your table's request rate. 
     */
    
    // ======== Query item using Local Secondary Index Key ===============
    private static void queryItems(String tableName, String indexName) throws Exception
    {
    	String orderId ="101";
		String CompanyName = "ABC Stationary Shop";
		String deliveryDate = "20141220";		
		
	    System.out.println("\n---------------------------------------------");
		System.out.println("QUERY TABLE: \n");
				    	
    	QueryRequest queryRequest = new QueryRequest().withTableName(tableName)
														.withConsistentRead(true)														
														.withScanIndexForward(true)
														.withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
    													// .withSelect("ALL_PROJECTED_ATTRIBUTES")
    													//.withSelect("COUNT") // Returns NoOf matching items, rather than matching items.    	
		/*
		 * Note: When performing a Query using LSI - 
		 * -> Must mention Hash Key associated with that Index 
		 * -> Conditions can not be more than 2.  
		 */
    	HashMap<String, Condition> keyConditions = new HashMap<String, Condition>();   // for Query Condition entry
		keyConditions.put("OrderId",new Condition().withComparisonOperator(ComparisonOperator.EQ)
													.withAttributeValueList(new AttributeValue().withN(orderId))); 
    	
		if (indexName == "index_CompanyName")
    	{    				
        	System.out.println("- Query table for Order ID: " + orderId + ", Company Name: " + CompanyName + ", using index: " + indexName);
        	
        	// had Predefined projected attributes.  
        	queryRequest.setSelect(Select.ALL_PROJECTED_ATTRIBUTES);
        	queryRequest.setIndexName(indexName);
    		   				
    		keyConditions.put("CompanyName",new Condition().withComparisonOperator(ComparisonOperator.EQ)
    														.withAttributeValueList(new AttributeValue().withS(CompanyName)));
    		            
    	}
		else if(indexName == "index_DeliveryDate")    	
    	{
        	System.out.println("- Query table for Order ID: " + orderId + ", Delivery Date is after: " + deliveryDate  + ", using index: " + indexName);
        	
        	// This has ALL as projected attribute but we will select few
        	queryRequest.setProjectionExpression("OrderId, OrderDate, CompanyName, CompanyAddress, DeliveryDate, DeliveryStatus, Due, Flagged");
    		queryRequest.setIndexName(indexName);
    		
    		keyConditions.put("DeliveryDate",new Condition().withComparisonOperator(ComparisonOperator.GT)
    														.withAttributeValueList(new AttributeValue().withN(deliveryDate)));
     
    	}
		else
		{
			System.out.println("- Query table for Order ID: " + orderId + " and No Index.");	
		}
		
		queryRequest.setKeyConditions(keyConditions);
		QueryResult result = dynamoDB.query(queryRequest);

        System.out.println("- Result: ");
        
		Iterator<Map<String, AttributeValue>> resultIter = result.getItems().iterator();
		while (resultIter.hasNext()) 
		{
			System.out.println("--- ");
			Map<String, AttributeValue> item = resultIter.next();	   			
			printItem(item);
			System.out.println("--- ");
		}
		
		System.out.println(" ");
        System.out.println("- Query Operation Status: ");
        System.out.println("- No of items evaluated, before QueryFilter is applied: " + result.getScannedCount());
        System.out.println("- No of items in response: " + result.getCount());
        System.out.println("- Capacity units consumed by this operation: " + result.getConsumedCapacity());	
		
    	/*
    	 List<Map<String, AttributeValue>> items = result.getItems();
		Iterator<Map<String, AttributeValue>> itemsIter = items.iterator();
		while (itemsIter.hasNext()) {
			Map<String, AttributeValue> currentItem = itemsIter.next();
			
			Iterator<String> currentItemIter = currentItem.keySet().iterator();
			while (currentItemIter.hasNext()) {
				String attr = (String) currentItemIter.next();
				if (attr == "OrderId" || attr == "IsOpen"
						|| attr == "OrderCreationDate") {
					System.out.println(attr + "---> "
							+ currentItem.get(attr).getN());
				} else {
					System.out.println(attr + "---> "
							+ currentItem.get(attr).getS());
				}
			}
			System.out.println();	
		}
		System.out.println("\nConsumed capacity: " + result.getConsumedCapacity() + "\n");
    	 */

    }

    // ======== Scan item with Condition: example ========================
    private static void scanItems(String tableName) throws Exception
    {    	
    	/* 
    	 * A Scan operation examines every item in the table. By default, a Scan returns all of the data attributes for every item; 
    	 * however, you can use the ProjectionExpression parameter so that the Scan only returns some of the attributes, rather than all of them.
    	 * A single Scan request can retrieve a maximum of 1 MB of data; DynamoDB can optionally apply a filter to this data, 
    	 * narrowing the results before they are returned to the user. A Scan operation always returns a result set, 
    	 * but if no matching items are found, the result set will be empty.
    	 * http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/QueryAndScan.html
    	 * 
    	 */
    	
    	/* Sequential Access !!
    	 * Even though DynamoDB distributes a large table's data across multiple physical partitions, a Scan operation can only read one partition 
    	 * at a time. For this reason, the throughput of a Scan is constrained by the maximum throughput of a single partition.
    	 * To address these issues, the Scan operation can logically divide a table into multiple segments, 
    	 * with multiple application workers scanning the segments in parallel. Each worker can be a thread 
    	 * (in programming languages that support multi-threading) or an operating system process. 
    	 * To perform a parallel scan, each worker issues its own Scan request with the following parameters: Segment, Total Segment.
    	 */

    	/* PAGINATING: 
    	 * If you query for specific attributes that match values that amount to more than 1 MB of data, you'll need to perform another 
    	 * Query request for the next 1 MB of data. To do this, take the LastEvaluatedKey value from the previous request, 
    	 * and use that value as the ExclusiveStartKey in the next request. 
    	 * This will let you progressively query or scan for new data in 1 MB increments.
    	 */
    	
    	boolean runScanBlock1 = true;
    	boolean runScanBlock2 = false;
    	boolean runScanBlock3 = false;
    	//boolean runScanBlock4 = true;
    	//boolean runScanBlock5 = true;

        try 
        {	
        	// Create an instance of the ScanRequest class and provide scan parameter.
        	ScanRequest scanRequest = new ScanRequest().withTableName(tableName);
        	ScanResult scanResult;
        	String columnName = null;
        	HashMap<String, Condition> scanFilter = new HashMap<String, Condition>();            
            
		    System.out.println("\n---------------------------------------------");
			System.out.println("SCAN TABLE: \n");			
			/*
			// Scans the entire table. The ScanRequest instance specifies the name of the table to scan.			
			ScanResult result = dynamoDB.scan(scanRequest);
			for (Map<String, AttributeValue> item : result.getItems())
			{
				printItem(item);				
			}
			*/			
			if (runScanBlock1)
			{
	            // Condition Type: 	Greater Than. 
	        	// Example: 		Scan items where Due is greater than zero
				columnName = "Due";
				String gtValue = "0";
	            Condition cond_dueGTzero = new Condition().withComparisonOperator(ComparisonOperator.GT.toString())
	            											.withAttributeValueList(new AttributeValue().withN(gtValue));
	            											
	            											
	            scanFilter.put(columnName, cond_dueGTzero);
	            scanRequest = new ScanRequest(tableName).withScanFilter(scanFilter);
	            										
	            scanResult = dynamoDB.scan(scanRequest);
	            
	            System.out.println("- Condition: Scan items where " + columnName + " is greater than " + gtValue + ".");
	            //System.out.println(scanResult);
	            // Scan Result Summary. 
				for (Map<String, AttributeValue> item : scanResult.getItems())
				{
					printItem(item);				
				}
	            System.out.println("\n- Scan Operation Status: ");
	            System.out.println("- No of items evaluated, before ScanFilter is applied: " + scanResult.getScannedCount());
	            System.out.println("- No of items in response: " + scanResult.getCount());
	            System.out.println("- Capacity units consumed by this operation: " + scanResult.getConsumedCapacity());				
			}

			if (runScanBlock2)
			{
	            // Condition: 	Between. 
	        	// Example: 	Scan items where DeliveryCost is between 50 & 100
				columnName = "DeliveryCost";
	        	Condition cond_dueBTWN = new Condition().withComparisonOperator(ComparisonOperator.BETWEEN.toString())
	        											.withAttributeValueList(new AttributeValue().withN("50"), new AttributeValue().withN("201"));
								
	            scanFilter.put(columnName, cond_dueBTWN);
	            scanRequest = new ScanRequest(tableName).withScanFilter(scanFilter);
	            scanResult = dynamoDB.scan(scanRequest);
	            System.out.println("- Condition: Scan items where DeliveryCost is between 50 & 100 \n" + scanResult);;
	            // Scan Result Summary. 
	            System.out.println("- Scan Operation Status: ");
	            System.out.println("- No of items evaluated, before ScanFilter is applied: " + scanResult.getScannedCount());
	            System.out.println("- No of items in response: " + scanResult.getCount());
	            System.out.println("- Capacity units consumed by this operation: " + scanResult.getConsumedCapacity());				
			}
			
			if (runScanBlock3)
			{
				Map<String, AttributeValue> expressionAttributeValues = new HashMap<String, AttributeValue>();
				expressionAttributeValues.put(":val", new AttributeValue().withN("10000"));
		        
		        scanRequest = new ScanRequest().withTableName(tableName).withFilterExpression("TotalCost < :val")
		        								.withExpressionAttributeValues(expressionAttributeValues)
		        								.withProjectionExpression("OrderId, OrderDate, CompanyName, CompanyContacts, TotalCost");

		        ScanResult result = dynamoDB.scan(scanRequest);
		        
		        System.out.println("Scan of " + tableName + " for items with a TotalCost less than 20,000");
		        //System.out.println(result.getItems());

		        for (Map<String, AttributeValue> item : result.getItems()) 
		        {
		            System.out.println("");
		            printItem(item);
		        }
			
			}			
            

            
        } 
        catch (AmazonServiceException ase) 
        {
        	printServiceExceptionError(ase);
        } 
        catch (AmazonClientException ace) 
        {
        	printClientExceptionError(ace);  
        }
    	
    }

    // ======== Method for Printing Item ===============================================    
    private static void printItem(Map<String, AttributeValue> attributeList) 
    {
        for (Map.Entry<String, AttributeValue> item : attributeList.entrySet()) 
        {
            String attributeName = item.getKey();
            AttributeValue value = item.getValue();
            
            // (X) Boolean Value not showing. Need to check later. 
            System.out.println(attributeName + " "
                    + (value.getS() == null ? "" : "[S] = " + value.getS())
                    + (value.getN() == null ? "" : "[N] = " + value.getN())
                    + (value.getB() == null ? "" : "[B] = " + value.getB())
                    + (value.getSS() == null ? "" : "[SS] = " + value.getSS())
                    + (value.getNS() == null ? "" : "[NS] = " + value.getNS())
                    + (value.getBS() == null ? "" : "[BS] = " + value.getBS() + "\n"));
        }
    }
    
    
    // ======== Get Item: example ========================================
	
    /*
     * http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.AccessingItemAttributes.html
     * To read an item from a DynamoDB table, use the GetItem operation. 
     * You must provide the name of the table, along with the primary key of the item you want. You need to specify 
     * the entire primary key. For example, if a table has a hash and range type primary key, you must supply a value for 
     * the hash attribute and a value for the range attribute.
     * The following are the default behaviors for GetItem: 
     * (1) performs eventually consistent read 
     * (2) returns all of the item's attributes 
     * (3) doesn't return any info about how many provisioned capacity units it consumes.
     * You can override these defaults using GetItem parameters.  You can optionally request a strongly consistent read instead; 
     * this will consume additional read capacity units, but it will return the most up-to-date version of the item.
     */
    private static void getItem(String tableName, String Id, String Orderdate) throws Exception
    {
    	String columnsToGet = "OrderId, OrderDate, CompanyName, DeliveryDate, DeliveryStatus, Due, Flagged, TotalCost";
    	System.out.println("\n---------------------------------------------");
    	System.out.println("GET ITEM: OrderId: " + Id + ", OrderDate: " +  Orderdate + "\n");
    	
    	try
    	{
    		HashMap<String, AttributeValue> key = new HashMap<String, AttributeValue>();
    		key.put("OrderId", new AttributeValue().withN(Id));
    		key.put("OrderDate", new AttributeValue().withN(Orderdate));
    		

    		GetItemRequest getItemRequest = new GetItemRequest().withTableName(tableName)
    															.withKey(key)
    															.withProjectionExpression(columnsToGet)
    															.withConsistentRead(true);
    															
    		GetItemResult result = dynamoDB.getItem(getItemRequest);
    		
    		Map<String, AttributeValue> item = result.getItem();
    		//System.out.println(item);
    		printItem(item);
    		System.out.println("- Capacity units consumed by this operation: " + result.getConsumedCapacity());	

    	}
        catch (AmazonServiceException ase) 
        {
        	printServiceExceptionError(ase);
        } 
        catch (AmazonClientException ace) 
        {
        	printClientExceptionError(ace);  
        }
    }
    
    
    // ==================================================================================
    // Section:  Miscellaneous 
    // Other methods ....  
    // ==================================================================================

    // ======== Print Amazon Service Exception Error =====================
    
    private static void printServiceExceptionError(AmazonServiceException ase1)
    {
    	System.out.println("\n(X) Error Occurred:-");
        System.out.println("- Application Message: Caught an AmazonServiceException, which means Request made it "
                + "to AWS, but was rejected with an error response for some reason.");
        System.out.println("- Error Message:    " + ase1.getErrorMessage());
        //System.out.println("Error Message:    " + ase1.getMessage());
        System.out.println("- HTTP Status Code: " + ase1.getStatusCode());
        System.out.println("- AWS Error Code:   " + ase1.getErrorCode());
        System.out.println("- Error Type:       " + ase1.getErrorType());
        System.out.println("- Request ID:       " + ase1.getRequestId());  
        //ase1.printStackTrace(System.err); // For details.
        
    }
    
    // ======== Print Amazon Client Exception Error ======================
    
    private static void printClientExceptionError(AmazonClientException ace1)
    {
    	System.out.println("\n(X) Error Occurred:-");
        System.out.println("- Application Message: Caught an AmazonClientException, which means the client encountered "
                + "a serious internal problem while trying to communicate with AWS, "
                + "such as not being able to access the network.");
        System.out.println("Error Message: " + ace1.getMessage());
    }

    // ======== Return Current Date Time as String =======================
    public static String GetCurrentDateTime() 
    {    	 
 	   //DateFormat dateFormat = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss");
    	DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
 	   //get current date time with Date()
 	   Date date = new Date(); 	   
 	   /*
 	    * get epoch time and convert it to Human readable form.
 	    * long epoch = new java.text.SimpleDateFormat("MM/dd/yyyy HH:mm:ss").parse("01/01/1970 01:00:00").getTime() / 1000;
 	    * 
 	    * -- get current date time with Calendar()
 	    * Calendar cal = Calendar.getInstance();
 	    * System.out.println(dateFormat.format(cal.getTime()));
 	    */
 	   return dateFormat.format(date).toString();
    }    
	
    // ======== Wait for Table Status =====================================
    // When deleting table we can use the DynamoDB API provided memthod or we can use this one.
    // http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/LowLevelJavaTableOperationsExample.html
    
    /*
        private static void waitForTableToBeDeleted(String tableName) 
        {
	        System.out.println("Waiting for " + tableName + " while status DELETING...");
	
	        long startTime = System.currentTimeMillis();
	        long endTime = startTime + (10 * 60 * 1000);
	        while (System.currentTimeMillis() < endTime) 
	        {
	            try 
	            {
	                DescribeTableRequest request = new DescribeTableRequest().withTableName(tableName);
	                TableDescription tableDescription = client.describeTable(request).getTable();
	                String tableStatus = tableDescription.getTableStatus();
	                System.out.println("  - current state: " + tableStatus);
	                if (tableStatus.equals(TableStatus.ACTIVE.toString())) return;
	            } 
	            catch (ResourceNotFoundException e) 
	            {
	                System.out.println("Table " + tableName + " is not found. It was deleted.");
	                return;
	            }
	            try {Thread.sleep(1000 * 20);} catch (Exception e) {}
	        }
        throw new RuntimeException("Table " + tableName + " was never deleted");
    	}
    */
    
    
}