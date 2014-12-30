/*
 * -----------------------------------------------------
 * Source: Amazon DynamoDB Developer guide, API guide.
 * Demo of DynamoDB provided all Low Level API actions
 * and how to do operations.
 * 
 * By: Ahsanul Hadi 
 * Email: ahsanulhadi.dsi@gmail.com, adil.gt@gmail.com
 * -----------------------------------------------------
 * Last update date: 30-DEC-2014
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
import com.amazonaws.services.dynamodbv2.util.Tables;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.dynamodbv2.document.*;


public class AmazonDynamoDBSample {

    static AmazonDynamoDBClient dynamoDB;
    static String tableName = "ProductOrder";   // For this Demo we are using a single table hence declared here.
	
    
    /* ====================================================================================
     * Main method.  
     * ====================================================================================
     */
    
    public static void main(String[] args) throws Exception 
    {

    	/* ==================== <Initialize DynamoDB connection> ====================== */
        init();  
        
        /* ==================== <Section: MANAGING Table> ============================= */

        //createTable(tableName); 		//Working OK.         
        //describeTable(tableName);   	//Working OK.        
        //updateTable(tableName);		//Working OK.        
        //listTable();     				//Working OK.        
        //deleteTable(tableName);		//Working OK.
                        
        /* ==================== <Section: CRUD Operations> ============================= */
        
        /* -------------------- CREATE/ Add items  ------------------------------------- */
        //addItems(tableName);   		//Working OK.
        //BatchWriteItems(tableName); 	//Working OK. 
   
        /* -------------------- READ/ Query, Scan, Get Items --------------------------- */                      
        String[][] search_key = new String[2][2];  					// --> sample SearchKeys for GetItem/ BatchGetItem. More keys can be added.  
        search_key[0][0] = "101";									//OrderId which is a Hash Key
        search_key[0][1] = "20141202094500";						//OrderDate which is a Range Key
        search_key[1][0] = "102";
        search_key[1][1] = "20141201094500";
        
        //getItem(tableName, search_key[0][0], search_key[0][1]); 	//Working OK. Search for One Item       
        //batchGetItem(tableName, search_key); 						//Working OK. Search for Multiple item hence passing the whole array.        
        //scanItems(tableName); 			    					//Working OK. No Key needed. Search based on defined Condition.
       
        String[][] search_key2 = new String[3][3];
        search_key2[0][0] = "index_DeliveryDate";					//Name of the Local Secondary Index or Global Secondary Index
        search_key2[0][1] = "20141228";								//For that Attribute: provide the search key value.
        search_key2[1][0] = "index_CompanyName";
        search_key2[1][1] = "ABC Stationary Shop";
        search_key2[2][0] = "null";
        search_key2[2][1] = "null";
        String orderId ="101";										//The HASH key. (RangeKey not needed here)
      
        //queryItems(tableName, search_key2[0][0], search_key2[0][1], orderId); //Working OK. Param: tableName, IndexName, SearchValue, HashId)
        //queryItems(tableName, search_key2[1][0], search_key2[1][1], orderId); //Working OK. 
        //queryItems(tableName, search_key2[2][0], search_key2[2][1], orderId); //Working OK. 

        /* -------------------- UPDATE/ Multiple Attribute, Conditional ---------------- */ 
    	String[] key = new String[]{"101","20141202094500"}; 		//Hash Key, Range Key
        //ConditionalUpdateItems(tableName, key); 					//Working OK. Will update that item ONLY IF condition met             
        //MultipleAttrUpdate(tableName, key); 						//Working OK.
        
    	/* -------------------- DELETE items ------------------------------------------ */ 
        key[0] = "101"; 											//Hash and Range Key 
        key[1] = "20141201094500";  
        //deleteItem(tableName, key);  								//Working OK. Will only delete the item if Condition is met.
        
    }

    /* ====================================================================================
     * Initialize DynamoDB Client with proper credentials 
     * ====================================================================================
     * The only information needed to create a client are security credentials consisting of the AWS Access Key ID and Secret Access Key. 
     * All other configuration, such as the service Endpoints, are performed automatically. 
     * Client parameters, such as proxies, can be specified in an optional ClientConfiguration object when constructing a client.
     *
     * @see com.amazonaws.auth.BasicAWSCredentials
     * @see com.amazonaws.auth.ProfilesConfigFile
     * @see com.amazonaws.ClientConfiguration
     */

    private static void init() throws Exception 
    {
        /*
         * The ProfileCredentialsProvider will return your [default] credential profile by reading from the credentials file located at
         * (C:\\Users\\<User Name>\\.aws\\credentials).
         * 
         * -- Another way to get credentials ----- 
    	 * AWSCredentials credentials = new PropertiesCredentials(AmazonDynamoDBSample.class.getResourceAsStream("AwsCredentials.properties"));
         * dynamoDB = new AmazonDynamoDBClient(credentials);
    	 */
        AWSCredentials credentials = null;
        try 
        {
            
        	credentials = new ProfileCredentialsProvider("TestUser").getCredentials();  // Using the AWS IAM user 'TestUser'
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

    // ===================================================================================
    // Section:  MANAGING TABLE - DynamoDB Low Level APIs
    // Actions: Create / Describe / Update / List / Delete table. 
    // ===================================================================================

    // ======== Create a Table ===========================================    
	private static void createTable(String tableName) throws Exception 
    {
		/* 
		 * http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithTables.html
		 * http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/SecondaryIndexes.html
		 * Must Read:
		 * http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GuidelinesForTables.html
		 */
		
		String hashItemName = "OrderId"; 				// will be used as HASH Key
		String rangeItemName = "OrderDate";  			// Will be used as a RANGE Key
		
		String LSIitem1 = "CompanyName";  				// will be used as Local Secondary Index #1
		String LSIname1 = "index_CompanyName"; 			// Local Secondary Index name. Will be projected on below 2 Attributes.
		String LSIitem1_ProjKey1 = "DeliveryDate"; 		// Projected Attribute for LSI #1
		String LSIitem1_ProjKey2 = "DeliveryStatus"; 	// Projected Attribute for LSI #1
		
		String LSIitem2 = "DeliveryDate"; 				// will be used as Local Secondary Index #2
		String LSIname2 = "index_DeliveryDate"; 		// Local Secondary Index name. will be projected on ALL Attributes.
		
		long RCU = 1L; 									// RCU = Read Capacity Units  
		long WCU = 1L; 									// WCU = Write Capacity Units
		
		
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
        		attributeDefinitions.add(new AttributeDefinition().withAttributeName(hashItemName).withAttributeType("N"));  	// Hash Key
        		attributeDefinitions.add(new AttributeDefinition().withAttributeName(rangeItemName).withAttributeType("N")); 	// Range Key	
        		attributeDefinitions.add(new AttributeDefinition().withAttributeName(LSIitem1).withAttributeType("S")); 		// Local Secondary Index 1
        		attributeDefinitions.add(new AttributeDefinition().withAttributeName(LSIitem2).withAttributeType("N")); 		// Local Secondary Index 2

        		// Note: Can not create Projected key Attributes now. Will not be allowed without Values.
        		
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
        		
        		// Projected Attributes for LSindex  i.e. Local Secondary Index #1
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
        		
        		// Projected Attributes for LSindex  i.e. Local Secondary Index #2
        		projection = new Projection().withProjectionType(ProjectionType.ALL);        		
        		LSindex2.setProjection(projection);
        		
        		localSecondaryIndexes.add(LSindex2);
        		
        		// Set the Index info
        		createTableRequest.setLocalSecondaryIndexes(localSecondaryIndexes);
        		// Finally CREATE the table.
        		CreateTableResult result = dynamoDB.createTable(createTableRequest);

        		/* ------------------ CREATE TABLE Section: End --------------------------------------- */
        		
        		System.out.println("- Current Table Status: " + result.getTableDescription().getTableStatus());	// Get current status of Table
        		// System.out.println(result.getTableDescription()); // Get Full Table Description 

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
		/*
		 * http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithTables.html#WorkingWithTables.ListingAndDescribing
		 */
		System.out.println("\n---------------------------------------------");
		System.out.println("DESCRIBE TABLE: \n");
		
    	try
    	{
            if (Tables.doesTableExist(dynamoDB, tableName))             	
            {
                DescribeTableRequest describeTableRequest = new DescribeTableRequest().withTableName(tableName);
                TableDescription tableDescription = dynamoDB.describeTable(describeTableRequest).getTable();  // Describe our new table    
                          
                System.out.println("Table Description:");
                System.out.println(tableDescription);

                /*// Details. 
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
		 * you might need to update these values. You can increase the read capacity units and write capacity units any time. 
		 * However, you can decrease these values ONLY 4 times in a 24 hour period. 
		 * For additional guidelines and limitations, see Specifying Read and Write Requirements for Tables.
		 * http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithTables.html#ProvisionedThroughput
		 */ 
		long RCU = 2L; // RCU = Read Capacity Units  
		long WCU = 2L; // WCU = Write Capacity Units
		long startTime = System.currentTimeMillis(); // For tracking consumed time.
		
		String tableStatusNow = null;
		String statusActive = "ACTIVE";
		
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
			
			if (!tableStatusNow.equals(statusActive))  // If table is not in Active status then no operation will be allowed.
			{
				System.out.println("(X) " + tableName + " is not Active yet. Try to update few minutes later.");
			}
			else
			{
				/*
				// This can be done as well.   
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
		        } while (!tableStatusNow.equals(statusActive));   // Stay in loop until table become Active.
		        
		        long stopTime = System.currentTimeMillis();
		        long elapsedTime = stopTime - startTime;	// Check how much time consumed. Typically not more than 7/8 second. 
		        		        
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
		 * Initially, exclusiveStartTableName is null, however, after fetching the first page of result, to retrieve the next page of result, 
		 * you must set this parameter value to the lastEvaluatedTableName property of the current result.
		 */
			
		String lastEvaluatedTableName = null;	// Initial value for the first page of table names.	
		int counter = 1;
		
		do 
		{
			// For this demo, Return maximum 10 table names.
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

	// ======== Delete Table ==============================================
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
    // Section:  CRUD Operations
    // Actions:  CREATE items.  
    // ==================================================================================
    
	/* 
	 * You can use AWS SDK for Java low-level API (protocol-level API) to perform typical create, read, update, and delete (CRUD) operations 
	 * on an item in a table. The Java API for item operations maps to the underlying DynamoDB API. For more information, see Using the DynamoDB API.
	 * 
	 * Note that the AWS SDK for Java also provides a high-level OBJECT PERSISTENCE MODEL, enabling you to map your client-side classes to 
	 * DynamoDB tables. This approach can reduce the amount of code you have to write. For more information, see Java: Object Persistence Model.
	 * More: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/LowLevelJavaItemCRUD.html
	 */
	
	// ======== Put item / Add item ========================================
    private static void addItems(String tableName) throws Exception
    {
    	/*
    	 * Creates a new item. If an item with the same key already exists in the table, it is replaced with the new item. 
    	 * http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithItems.html#WorkingWithItems.WritingData
    	 */
    	
    	String[] ProductId_list= new String[]{"M001","M002","P001"};  // sample productID. to add String Set type.

    	String CompanyAddress = "{"
    			+   "\"Street\": \"Road #1, Section #1, ABC Area. Post Code: 1001.\","
    			+   "\"City\":\"Dhaka\","
    			+   "\"Country\": \"Bangladesh\""
    			+   "},";
			
    	PutItemRequest itemRequest;
    	PutItemResult putItemResult;
    	// GetCurrentDateTime();    
    	System.out.println("\n---------------------------------------------");
    	System.out.println("PUT/ADD ITEM: \n");
    
    	try 
        {
			Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();       				
			// Sample status = Delivered, Pending, Halt, Returned, Missing ....
			// int orderDate = Integer.parseInt(GetCurrentDateTime());
			long orderDate = 20141201090909L;
			String DeliveryDate = "20141125";
			
			/* Param:
			 * int OrderId, int OrderDate, String[] ProductId_list, String DeliveryDate, String DeliveryStatus, String DeliveredBy,
			 * int DeliveryCost, int DiscountAmount, String TotalCost, int Due, String CompanyName, String json_CompanyAddress, boolean Flagged) 
			 */			
        	item = newItem(101, orderDate, ProductId_list, DeliveryDate, "Pending", "S.A. Paribahan", 
        			20, 0, "1000", 200, "ABC Stationary Shop", CompanyAddress, false);
        	
        	
            itemRequest = new PutItemRequest(tableName, item);   // Adding one item. Repeat process if needed.
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

	// ======== BATCH Write: Put multiple item & Delete them ===============
    private static void BatchWriteItems(String tableName) throws Exception
    {
    	
    	/*
    	 * You should check if there were any UNPROCESSED request items returned in the response. This could happen if you reach the 
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
    	
    	/* CONDITIONS: 
    		If one or more of the following is true, DynamoDB rejects the entire batch write operation:
    		- One or more tables specified in the BatchWriteItem request does not exist.
    		- Primary key attributes specified on an item in the request do not match those in the corresponding table's primary key schema.
    		- You try to perform multiple operations on the SAME ITEM in the same BatchWriteItem request. For example, you cannot put and delete 
    		the same item in the same BatchWriteItem request.
    		- There are more than 25 requests in the batch.
    		- Any individual item in a batch exceeds 400 KB.
    		- The total request size exceeds 16 MB.
    	*/
    	
    	String[] ProductId_list= new String[]{"M001","M002","P001"}; 

    	String CompanyAddress = "{"
    			+   "\"Street\": \"Road #1, Section #1, ABC Area. Post Code: 1001.\","
    			+   "\"City\":\"Dhaka\","
    			+   "\"Country\": \"Bangladesh\""
    			+   "},";

		long orderDate;
		String DeliveryDate;
		String hashItemName = "OrderId"; 		// will be used as HASH Key
		String rangeItemName = "OrderDate";  	// Will be used as a RANGE Key
    	System.out.println("\n---------------------------------------------");
    	System.out.println("BATCH Write Operations: \n");
		
    	try
    	{
    		// Create a map for the requests in the batch. Table Name and job List.
    		Map<String, List<WriteRequest>> requestItems = new HashMap<String, List<WriteRequest>>();
    		List<WriteRequest> jobList = new ArrayList<WriteRequest>(); // Create an Array List to add Tasks in the Job List. 	        
    		Map<String, AttributeValue> item = new HashMap<String, AttributeValue>(); // for Adding item. 
    		
    		/*
    		item.put("Attribute1", new AttributeValue().withS("Value ..."));
    		item.put("Attribute2", new AttributeValue().withN("Value"));
    		*/

    		orderDate = 20141202094500L;
    		DeliveryDate = "20141220";

    		/* Param:
    		 * int OrderId, int OrderDate, String[] ProductId_list, String DeliveryDate, String DeliveryStatus, String DeliveredBy,
    		 * int DeliveryCost, int DiscountAmount, String TotalCost, int Due, String CompanyName, String json_CompanyAddress, boolean Flagged) 
    		 */
        	item = newItem(102, orderDate, ProductId_list, DeliveryDate, "Delivered", "S.A. Paribahan", 
        				   100, 0, "3000", 0, "ABC Stationary Shop", CompanyAddress, false);
        	
    		jobList.add(new WriteRequest().withPutRequest(new PutRequest().withItem(item)));   // Add write request #1
    		
    		orderDate = 20141201094500L;
    		DeliveryDate = "20141220";
    		
        	item = newItem(103, orderDate, ProductId_list, DeliveryDate, "Delivered", "S.A. Paribahan", 
        				   0, 0, "3000", 0, "ABC Stationary Shop", CompanyAddress, false);
        	
    		jobList.add(new WriteRequest().withPutRequest(new PutRequest().withItem(item))); // Add write request #2
    		
    		//requestItems.put(tableName, jobList);
    		// We can add items to a other tables in the same way. 
    		
    		// Now Create a DeleteRequest 
    		HashMap<String, AttributeValue> DeleteKey = new HashMap<String, AttributeValue>();
    		DeleteKey.put(hashItemName, new AttributeValue().withN("101"));
    		DeleteKey.put(rangeItemName, new AttributeValue().withN("20141202094500"));
    		    
    		jobList.add(new WriteRequest().withDeleteRequest(new DeleteRequest().withKey(DeleteKey))); // Add Delete request #1
    		
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
                
                
               /* 
                // Process the failed ones again.
                Map<String, List<WriteRequest>> unprocessedItems = result.getUnprocessedItems();
                
                if (result.getUnprocessedItems().size() == 0) 
                 {
                	 System.out.println("No unprocessed items found");
                 }
                else 
                 {
                	 System.out.println("Retrieving the unprocessed items");
                	 //result = dynamoDB.batchWriteItemUnprocessed(unprocessedItems);
                	 result = dynamoDB.batchWriteItemUnprocessed(requestItems);
				}
               */

            } while (result.getUnprocessedItems().size() > 0); 	// Run as long as there are Unprocessed items. 	
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
    
    // ======== Method for adding items in Hash map ========================   
    private static Map<String, AttributeValue> newItem(int OrderId, long OrderDate, String[] ProductId_list, 
    		String DeliveryDate, String DeliveryStatus, String DeliveredBy, int DeliveryCost, int DiscountAmount, 
    		String TotalCost, int Due, String CompanyName, String CompanyAddress, boolean Flagged)
    {
    	// This method adds Attribute values to the item map. 
		// private static Map<String, AttributeValue> newItem(String name, int year, String rating, String... fans)        
    	
    	Map<String, AttributeValue> item1 = new HashMap<String, AttributeValue>();
		// Add the items
    	item1.put("OrderId", new AttributeValue().withN(Integer.toString(OrderId)));
    	item1.put("OrderDate", new AttributeValue().withN(Long.toString(OrderDate)));
    	item1.put("ProductId_list", new AttributeValue().withSS(ProductId_list));	
    	item1.put("DeliveryDate", new AttributeValue().withN(DeliveryDate));
    	item1.put("DeliveryStatus", new AttributeValue(DeliveryStatus));
		item1.put("DeliveredBy", new AttributeValue().withS(DeliveredBy));			
		item1.put("DeliveryCost", new AttributeValue().withN(Integer.toString(DeliveryCost)));
		item1.put("DiscountAmount", new AttributeValue().withN(Integer.toString(DiscountAmount)));
		item1.put("TotalCost", new AttributeValue().withN(String.valueOf(TotalCost)));
		item1.put("Due", new AttributeValue().withN(String.valueOf(Due)));		
		item1.put("CompanyName", new AttributeValue().withS(CompanyName));				
		item1.put("CompanyAddress", new AttributeValue().withS(CompanyAddress));
		item1.put("Flagged", new AttributeValue().withBOOL(Flagged));
        //item.put("fans", new AttributeValue().withSS(fans));
		//item.put("Authors", new AttributeValue().withSS(Arrays.asList("Author1", "Author2")));
        return item1;
    }

    
    // ==================================================================================
    // Section:  CRUD Operations
    // Actions:  READ items.  
    // ==================================================================================

    /* <NOTE>
     * Query: 	 Hash Key must be provided. Directly use Keys map/ Index. Doesn't scan full table.
     * Scan:  	 Hash key not needed. Can search on any Attribute. Does a full scan. Costly.
     * Get Item: Hash and Range key must be provided. 
     * 
     * Generally, a Query operation is more efficient (with limitations) than a Scan operation. 
     * A Scan operation SCANS ENTIRE TABLE, then filters out values to provide the desired result, essentially adding the extra step of 
     * removing data from the result set. 
     * 
     * Avoid using a Scan operation on a large table with a filter that removes many results, if possible. 
     * Also, as a table grows, the Scan operation SLOWS. 
     * The Scan operation examines every item for the requested values, and can use up the provisioned throughput for a large table 
     * in a single operation. For quicker response times, design your tables in a way that can use the Query, Get, or BatchGetItem APIs, instead. 
     * Alternatively, design your application to use Scan operations in a way that minimizes the impact on your table's request rate. 
     */
    
    /*
     * Specifying Conditions: 
     * http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html#ExpressionAttributeValues
     * 
     *  Using Document path:
     *  http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.AccessingItemAttributes.html
     */
    
    // ======== Query item using Local Secondary Index Key ================
    private static void queryItems(String tableName, String indexName, String search_key, String orderId) throws Exception
    {		
    	/*
    	 * http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/QueryAndScan.html
    	 * 
    	 * A Query operation finds items in a table using ONLY primary key attribute values. You must provide a hash key distinct value to search for. 
    	 * You can optionally provide a range key attribute name and value, and use a comparison operator to refine the search results. 
    	 * Query supports a specific set of comparison operators for choosing key values. 
    	 * You must specify the hash key attribute name and value as an equality condition. 
    	 * You can optionally specify a second condition, referring to the range key attribute; this condition allows you to choose from several 
    	 * conditional operators. 
    	 * 
    	 * Available comparison operators: See KeyConditions section.
    	 * http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#API_Query_RequestSyntax 
    	 * 
    	 * If your table has one or more Secondary indexes, you can Query those indexes in the same way that you query a table.
    	 * -> Must mention Hash Key associated with that Index  (if using Global Secondary Index)
    	 * -> Conditions can not be more than 2.
    	 */
		
	    System.out.println("\n---------------------------------------------");
		System.out.println("QUERY TABLE: \n");
				    	
    	QueryRequest queryRequest = new QueryRequest().withTableName(tableName)
														.withConsistentRead(true)														
														.withScanIndexForward(true)
														.withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
    													// .withSelect("ALL_PROJECTED_ATTRIBUTES")
    													//.withSelect("COUNT") // Returns NoOf matching items, rather than matching items.    	

    	HashMap<String, Condition> keyConditions = new HashMap<String, Condition>();   // for Query Condition entry
    	
    	// Specifying Hash Key EQ comparison which is mandatory and will be same for all.
		keyConditions.put("OrderId",new Condition().withComparisonOperator(ComparisonOperator.EQ)
													.withAttributeValueList(new AttributeValue().withN(orderId))); 
    	
		if (indexName == "index_CompanyName")  // Query using Local Secondary Index #1 on Company Name
    	{  
			// note: design wise index shouldn't be on Company Name. This is just used for demo.  
        	System.out.println("- Query table for Order ID: " + orderId + ", Company Name: " + search_key + ", using index: " + indexName);
        	
        	// had Predefined projected attributes.  
        	queryRequest.setSelect(Select.ALL_PROJECTED_ATTRIBUTES);
        	queryRequest.setIndexName(indexName);
    		   				
    		keyConditions.put("CompanyName",new Condition().withComparisonOperator(ComparisonOperator.EQ)
    														.withAttributeValueList(new AttributeValue().withS(search_key)));
    		            
    	}
		else if(indexName == "index_DeliveryDate")  // Query using Local Secondary Index #2 on Delivery Date  	
    	{
			// If you need to do Comparison operation then Date must be set as Number and must have a defined format.
        	System.out.println("- Query table for Order ID: " + orderId + ", Delivery Date is after: " + search_key  + ", using index: " + indexName);
        	
        	// This has ALL as projected attribute but we will select few
        	queryRequest.setProjectionExpression("OrderId, OrderDate, CompanyName, CompanyAddress, DeliveryDate, DeliveryStatus, Due, Flagged");
    		queryRequest.setIndexName(indexName);
    		
    		keyConditions.put("DeliveryDate",new Condition().withComparisonOperator(ComparisonOperator.GT)
    														.withAttributeValueList(new AttributeValue().withN(search_key)));
     
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

    // ======== Scan item with Condition: example =========================
    private static void scanItems(String tableName) throws Exception
    {    	
    	/* 
    	 * A Scan operation examines EVERY ITEM in the table. By default, a Scan returns all of the data attributes for every item; 
    	 * Use the ProjectionExpression parameter so that the Scan only returns some of the attributes, rather than all of them.
    	 * A single Scan request can retrieve a maximum of 1 MB of data; 
    	 * A Scan operation always returns a result set but if no matching items are found, the result set will be empty.
    	 * http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/QueryAndScan.html
    	 */
    	
    	/* <How to avoid Sequential Access>
    	 * Even though DynamoDB distributes a large table's data across multiple physical partitions, a Scan operation can only read one partition 
    	 * at a time. For this reason, the throughput of a Scan is constrained by the maximum throughput of a single partition.
    	 * To address these issues, the Scan operation can logically divide a table into multiple segments, with multiple application workers 
    	 * scanning the segments in parallel. Each worker can be a thread (in programming languages that support multi-threading) or 
    	 * an operating system process. To perform a parallel scan, each worker issues its own Scan request with the following parameters: 
    	 * Segment, Total Segment.
    	 */

    	/* PAGINATING: 
    	 * If you query for specific attributes that match values that amount to more than 1 MB of data, you'll need to perform another 
    	 * Query request for the next 1 MB of data. To do this, take the LastEvaluatedKey value from the previous request, and use that 
    	 * value as the ExclusiveStartKey in the next request. This will let you progressively query or scan for new data in 1 MB increments.
    	 */
    	
    	// These are specific Condition based block. 
    	// Enable/Disable whichever block you want to run for this demo. Can execute all of these 3 as well. 
    	boolean runScanBlock1 = true;
    	boolean runScanBlock2 = false;
    	boolean runScanBlock3 = false;


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
	            // Condition: 	Less than. 
	        	// Example: 	Scan items where TotalCost is less than provided value
				
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

    // ======== Method for Printing Item Attributes =======================    
    private static void printItem(Map<String, AttributeValue> attributeList) 
    {
    	Boolean stat = true; 
    	
        for (Map.Entry<String, AttributeValue> item : attributeList.entrySet()) 
        {
            String attributeName = item.getKey();
            AttributeValue value = item.getValue();
            
            // (X) Boolean Value not showing. Need to check later. 
            System.out.println(attributeName + " "
                    + (value.getS() == null ? "" : "[S] = " + value.getS())
                    + (value.getN() == null ? "" : "[N] = " + value.getN())
                    //+ (value.getB() == null ? "" : "[B] = " + value.getB())
                    + (value.getBOOL() == null ? "" : "[B] = " + value.getBOOL())
                    + (value.getSS() == null ? "" : "[SS] = " + value.getSS())
                    + (value.getNS() == null ? "" : "[NS] = " + value.getNS())
                    + (value.getBS() == null ? "" : "[BS] = " + value.getBS() + "\n"));
        }
    }
    
    
    // ======== Get Item: example =========================================	

    private static void getItem(String tableName, String Id, String Orderdate) throws Exception
    {
        /*
         * http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithItems.html#WorkingWithItems.ReadingData
         * http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.AccessingItemAttributes.html
         * 
         * To read an item from a DynamoDB table, use the GetItem operation. 
         * You must provide the name of the table, along with the primary key (Hash and Range, both) of the item you want.
         * The following are the default behaviors for GetItem: 
         * (1) performs eventually consistent read 
         * (2) returns all of the item's attributes 
         * (3) doesn't return any info about how many provisioned capacity units it consumes.
         * You can override these defaults using GetItem parameters.  You can optionally request a strongly consistent read instead; 
         * this will consume additional read capacity units, but it will return the most up-to-date version of the item.
         */
    	
    	String columnsToGet = "OrderId, OrderDate, CompanyName, DeliveryDate, DeliveryStatus, Due, Flagged, TotalCost";
    	System.out.println("\n---------------------------------------------");
    	System.out.println("GET ITEM: OrderId: " + Id + ", OrderDate: " +  Orderdate + "\n");
    	
    	try
    	{
    		HashMap<String, AttributeValue> key = new HashMap<String, AttributeValue>();
    		key.put("OrderId", new AttributeValue().withN(Id));				// Hash Key
    		key.put("OrderDate", new AttributeValue().withN(Orderdate));	// Range Key
    		

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
    
    // ======== BATCH Get Item: example ===================================	

    private static void batchGetItem(String tableName, String[][] search_key) throws Exception
    {
        
        /*
         *  The BatchGetItemRequest specifies the table names and item PK key list for each item to get. 
         *  http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/batch-operation-document-api-java.html#LowLevelJavaBatchWrite
         *  
         *  Along with the required parameters, you can also specify optional parameters when using batchGetItem. 
         *  For example, you can provide a ProjectionExpression with each TableKeysAndAttributes you define. 
         *  This allows you to specify the attributes that you want to retrieve from the table.
         */
    	
    	//String columnsToGet = "OrderId, OrderDate, CompanyName, DeliveryDate, DeliveryStatus, Due, Flagged, TotalCost";
    	System.out.println("\n---------------------------------------------");
    	System.out.println("BATCH GET ITEM: \n");    	
    	
        BatchGetItemResult result;
        BatchGetItemRequest batchGetItemRequest = new BatchGetItemRequest();
        
    	Map<String, KeysAndAttributes> requestItems = new HashMap<String, KeysAndAttributes>();
    	
    	List<Map<String, AttributeValue>> tableKeys = new ArrayList<Map<String, AttributeValue>>(); 
        
    	Map<String, AttributeValue> key = new HashMap<String, AttributeValue>(); // For item #1
        key.put("OrderId", new AttributeValue().withN(search_key[0][0]));   // Hash Key
        key.put("OrderDate", new AttributeValue().withN(search_key[0][1]));  // Range Key
        tableKeys.add(key);
        
        key = new HashMap<String, AttributeValue>(); // For item #2
        key.put("OrderId", new AttributeValue().withN(search_key[1][0]));  
        key.put("OrderDate", new AttributeValue().withN(search_key[1][1]));
        tableKeys.add(key);
                
        requestItems.put(tableName, new KeysAndAttributes().withKeys(tableKeys)); 
        
        /*  Iterate this for another Table. 
        tableKeys = new ArrayList<Map<String, AttributeValue>>();
        
        key = new HashMap<String, AttributeValue>();
        key.put("OrderId", new AttributeValue().withN("101"));  
        key.put("OrderDate", new AttributeValue().withN("20141201094500"));
        tableKeys.add(key);   
        
        requestItems.put(table2Name, new KeysAndAttributes().withKeys(tableKeys));
    	*/
        
    	try
    	{
            do 
            {
                System.out.println("- Making the request.");
                                
                batchGetItemRequest.withRequestItems(requestItems);
                result = dynamoDB.batchGetItem(batchGetItemRequest);
                                
                List<Map<String, AttributeValue>> table1Results = result.getResponses().get(tableName); // for table 1 
                //List<Map<String, AttributeValue>> table2Results = result.getResponses().get(table2Name); // for table 2 
                
                if (table1Results != null)
                {
                    System.out.println("- Items in table " + tableName);
                    for (Map<String,AttributeValue> item : table1Results) 
                    {
                    	System.out.println("---- ");
                        printItem(item);
                        
                    }
                }
                
                /*  
                 // Iterate this for another Table. 
                 
                List<Map<String, AttributeValue>> table2Results = result.getResponses().get(table2Name);
                if (table2Results != null)
                {
                    System.out.println("\nItems in table " + table2Name);
                    for (Map<String,AttributeValue> item : table2Results) 
                    {
                        printItem(item);
                    }
                }
                */
                
                // Check for unprocessed keys which could happen if you exceed provisioned throughput or reach the limit on response size. 
                
                for (Map.Entry<String,KeysAndAttributes> pair : result.getUnprocessedKeys().entrySet()) 
                {
                    System.out.println("- Unprocessed key pair: " + pair.getKey() + ", " + pair.getValue());
                }
                
                requestItems = result.getUnprocessedKeys();
                
                /*
				// Check for unprocessed keys which could happen if you exceed provisioned throughput or reach the limit on response size.
				
				Map<String, KeysAndAttributes> unprocessedKeys = result.getUnprocessedKeys();
			
				if (result.getUnprocessedKeys().size() == 0) 
				{
					System.out.println("No unprocessed keys found");
				} 
				else 
				{
					System.out.println("Retrieving the unprocessed keys");
					result = dynamoDB.batchGetItemUnprocessed(unprocessedKeys);
				}
				
				*/
                
            } while (result.getUnprocessedKeys().size() > 0);
    	
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
    // Section:  CRUD Operations
    // Actions:  UPDATE items.  
    // ==================================================================================

    /*
     * Modifying Attributes:
     * http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Modifying.html
     */
    
	// ======== Conditional add/update item ================================
    private static void ConditionalUpdateItems(String tableName, String[] key) throws Exception
    {
    	/*
    	 * Use an optional parameter to specify a condition for uploading the item. 
    	 * If the condition specified is not met, then the AWS Java SDK throws a ConditionalCheckFailedException. 
    	 * http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/LowLevelJavaItemCRUD.html
    	 * 
    	 * Sample Condition used here:
    	 * It will only allow any update, when the existing DeliveryStatus attribute value is "Pending". 
    	 * Otherwise, it will raise error: ConditionalCheckFailedException
    	 */
    	String[] ProductId_list= new String[]{"M001","M002","P001"}; 

    	String CompanyAddress = "{"
    			+   "\"Street\": \"Road #1, Section #1, ABC Area. Post Code: 1001.\","
    			+   "\"City\":\"Dhaka\","
    			+   "\"Country\": \"Bangladesh\""
    			+   "},";

    	PutItemRequest itemRequest;
    	PutItemResult putItemResult;
    	// GetCurrentDateTime()
    
    	System.out.println("\n---------------------------------------------");
    	System.out.println("PUT ITEM with Condition: \n");
    
    	try 
        {
			Map<String, AttributeValue> item = new HashMap<String, AttributeValue>(); // for Adding item. 
			
			/* Param:
			 * int OrderId, int OrderDate, String[] ProductId_list, String DeliveryDate, String DeliveryStatus, String DeliveredBy,
			 * int DeliveryCost, int DiscountAmount, String TotalCost, int Due, String CompanyName, String json_CompanyAddress, boolean Flagged) 
			 */
        	item = newItem(Integer.parseInt(key[0]), Long.parseLong(key[1]), ProductId_list, GetCurrentDateTime(), "Pending", "S.A. Paribahan", 
        			100, 0, "5000", 900, "XYZ Stationary Shop", CompanyAddress, false);
        	
			Map<String, AttributeValue> expressionAttributeValues = new HashMap<String, AttributeValue>(); // for condition expression.
			expressionAttributeValues.put(":val", new AttributeValue().withS("Pending")); // for Delivery Status.  
			//expressionAttributeValues.put(":val", new AttributeValue().withN("0")); // for Due

            itemRequest = new PutItemRequest().withTableName(tableName).withItem(item)
            															.withConditionExpression("DeliveryStatus = :val")
            															.withExpressionAttributeValues(expressionAttributeValues)
            															.withReturnValues(ReturnValue.ALL_OLD);
            putItemResult= dynamoDB.putItem(itemRequest);
            //System.out.println("Add item Result: " + putItemResult);            
            item.clear();                     	
 
    		Map<String, AttributeValue> outputItem = putItemResult.getAttributes();
    		printItem(outputItem);
    				
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
    
	// ======== Multiple Attribute Update ==================================
    
    private static void MultipleAttrUpdate(String tableName, String[] key) throws Exception
    {
    	try
    	{
            Map<String, AttributeValueUpdate> updateItems = new HashMap<String, AttributeValueUpdate>();

			HashMap<String, AttributeValue> keyValue = new HashMap<String, AttributeValue>();
			keyValue.put("OrderId", new AttributeValue().withN(key[0]));
			keyValue.put("OrderDate", new AttributeValue().withN(key[1]));
			 
			/* -------------------------------------------
			 * withAction(AttributeAction...) Values --> 
			 * Specifies how to perform the update. Valid values are PUT (default), DELETE, and ADD. 
			 * The behavior depends on whether the specified primary key already exists in the table. 
			 * This action is only valid for an existing attribute whose data type is number or is a set. Do not use ADD for any other data types.
			 * -------------------------------------------
			 * ADD: 
			 * -> If the attribute does not exist, then the attribute and its values are added to the item. 
			 * -> If the attribute does exist, then the behavior of ADD depends on the data type of the attribute: 
			 * ----> If it's a Positive number, and if Value is also a number, then the Value is mathematically added to the existing attribute. 
			 * ----> If it's a Negative number, then it is subtracted from the existing attribute. 
			 * ----> If using ADD to increment/decrement a num value for an item that doesn't exist before update, DynamoDB uses 0 as initial value.
			 * For example: below code ADDs 300 with current Due. 
			 */
			 updateItems.put("Due", new AttributeValueUpdate().withAction(AttributeAction.ADD).withValue(new AttributeValue().withN("300")));
			 
			/* 
			 * ----> If it's a SET, and if the Value is also a set, then the Value is added to the existing set. 
			 * For example: if the attribute value was a set [1,2] and ADD action specified [3], then the final attribute value would be [1,2,3]. 
			 * For example: Below code ADDs 2 new Product ID to the existing Set. 
			 */
			 
			 updateItems.put("ProductId_list", new AttributeValueUpdate().withAction(AttributeAction.ADD).withValue(new AttributeValue().withSS("M003", "SP01")));
			
			/* -------------------------------------------
			 * PUT:
			 * -> If an item with the specified Key is found: Then it Adds the specified attribute to the item. 
			 * -> If the attribute already exists, then it is REPLACED by the new value. 
			 * For example: below code Update TotalCost. Replaced old value with the new one.
			 */
			 updateItems.put("TotalCost", new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(new AttributeValue().withN("0")));
			 
			/* 
			 * -> If no item with the specified Key is found: then it creates a new item with the specified PKey and then adds the attribute. 
			 * For example: below code adds "NewAttribute" cause it doesn't exist for the item. 
			 * updateItems.put("NewAttribute", new AttributeValueUpdate().withValue(new AttributeValue().withS("someValue")));  
			 * OR   */
			 updateItems.put("NewAttribute", new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(new AttributeValue().withS("someValue")));

			/* -------------------------------------------
			 * DELETE: 
			 * -> If an item with the specified Key is found in the table: 
			 * -----> If no value is specified, then the attribute and its value are removed from the item. 
			 * For example: below code deletes the "NewAttribute" attrib. 
			 */
			 // updateItems.put("NewAttribute", new AttributeValueUpdate().withAction(AttributeAction.DELETE));
			 
			/*
			 * -> If no item with the specified Key is found:
			 * -----> Nothing happens; there is no attribute to delete.
			 * ---------------------------------------------
			 */


			// Now EXECUTE the UPDATE operation 
			UpdateItemRequest updateItemRequest = new UpdateItemRequest().withTableName(tableName)
																		 .withKey(keyValue)
																		 .withAttributeUpdates(updateItems)
																		 .withReturnValues(ReturnValue.ALL_NEW);
			
			UpdateItemResult result = dynamoDB.updateItem(updateItemRequest);
			
			// Check the response.
			System.out.println("- Printing item after multiple attribute update...");
			printItem(result.getAttributes());            
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
    // Section:  CRUD Operations
    // Actions:  DELETE items.  
    // ==================================================================================
    
    private static void deleteItem(String tableName, String[] key) throws Exception
    {
		System.out.println("\n---------------------------------------------");
		System.out.println("DELETE Item: Where OrderId= " + key[0] + " and Order Date = " + key[1] + "\n");

		try
		{
			// Hash and Range key for that Table.
			Map<String, AttributeValue> keyValues = new HashMap<String, AttributeValue>();
			keyValues.put("OrderId", new AttributeValue().withN(key[0]));
			keyValues.put("OrderDate", new AttributeValue().withN(key[1]));

			// Conditional Value Map			
			Map<String, AttributeValue> expressionAttributeValues = new HashMap<String, AttributeValue>(); // for condition expression.
			expressionAttributeValues.put(":val", new AttributeValue().withS("Delivered"));			

			// create our request
			// Here we will delete an Item only when the condition is met i.e. DeliveryStatus value has to be "Delivered". 
			DeleteItemRequest deleteItemRequest = new DeleteItemRequest().withTableName(tableName)
																		 .withKey(keyValues)
																		 .withConditionExpression("DeliveryStatus = :val")
																		 .withExpressionAttributeValues(expressionAttributeValues)
																		 .withReturnValues(ReturnValue.ALL_OLD);
																		 
			// process the delete request
			DeleteItemResult result = dynamoDB.deleteItem(deleteItemRequest);
            System.out.println("- Printing item that was deleted...");
            printItem(result.getAttributes());
			
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