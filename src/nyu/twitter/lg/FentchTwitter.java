package nyu.twitter.lg;

/*
 * Copyright 2012-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
import java.io.IOException;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.swing.JOptionPane;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.util.Tables;



import twitter4j.FilterQuery;
import twitter4j.Place;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.User;
import twitter4j.conf.ConfigurationBuilder;

/**
 * This sample demonstrates how to perform a few simple operations with the
 * Amazon DynamoDB service.
 */
public class FentchTwitter {

	/*
	 * WANRNING: To avoid accidental leakage of your credentials, DO NOT keep
	 * the credentials file in your source directory.
	 */

	static AmazonDynamoDBClient dynamoDB;
	static String tableName = "TweetTable";
	private static double latitude;
	private static double longtitude;
	private static int count = 1;
	private static String name;
	private static String place;
	private static String message;
	private static String id;
	private static String date;

	/**
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
	private static void init() throws Exception {
		/*
		 * The ProfileCredentialsProvider will return your [New US East
		 * (Virginia) Profile] credential profile by reading from the
		 * credentials file located at ().
		 */
//		AWSCredentials credentials = null;
		 AWSCredentialsProvider credentialsProvider= null;

		try {
			credentialsProvider =  new ClasspathPropertiesFileCredentialsProvider();
//			credentials = new ProfileCredentialsProvider(
//					"New US East (Virginia) Profile").getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException(
//					"Cannot load the credentials from the credential profiles file. "
//							+ "Please make sure that your credentials file is at the correct "
//							+ "location (), and is in valid format.",
					e);
		}
		dynamoDB = new AmazonDynamoDBClient(credentialsProvider);
		Region usEast1 = Region.getRegion(Regions.US_EAST_1);
		dynamoDB.setRegion(usEast1);
	}

	public static void invoke() throws Exception {
		init();
		// Create table if it does not exist yet
		if (Tables.doesTableExist(dynamoDB, tableName)) {
			System.out.println("Table " + tableName + " is already ACTIVE");
		} else {
			// Create a table with a primary hash key named 'name', which holds
			// a string
			CreateTableRequest createTableRequest = new CreateTableRequest()
					.withTableName(tableName)
					.withKeySchema(
							new KeySchemaElement().withAttributeName("id")
									.withKeyType(KeyType.HASH))
					.withAttributeDefinitions(
							new AttributeDefinition().withAttributeName("id")
									.withAttributeType(ScalarAttributeType.S))
					.withProvisionedThroughput(
							new ProvisionedThroughput().withReadCapacityUnits(
									1L).withWriteCapacityUnits(1L));
			TableDescription createdTableDescription = dynamoDB.createTable(
					createTableRequest).getTableDescription();
			System.out.println("Created Table: " + createdTableDescription);
			// Wait for it to become active
			System.out.println("Waiting for " + tableName
					+ " to become ACTIVE...");
			Tables.waitForTableToBecomeActive(dynamoDB, tableName);
		}

		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
				.setOAuthConsumerKey("Emwo2pG")
				.setOAuthConsumerSecret(
						"RM9B7fske5T")
				.setOAuthAccessToken(
						"19ubQOirq")
				.setOAuthAccessTokenSecret(
						"Lbg3C");

		final TwitterStream twitterStream = new TwitterStreamFactory(cb.build())
				.getInstance();

		StatusListener listener = new StatusListener() {

			@Override
			public void onStatus(Status status) {
	
				if (status.getGeoLocation() != null
						&& status.getPlace() != null) {
					
//					if (count == 0) {
//						count++;
//					}
//					
					latitude = status.getGeoLocation().getLatitude();
					longtitude = status.getGeoLocation().getLongitude();
					place = status.getPlace().getCountry() + ","
							+ status.getPlace().getFullName();
					date = status.getCreatedAt().toString();
					id = Integer.toString(count);
					name = status.getUser().getScreenName();
					message = status.getText();
					System.out.println("---------------------------");
					System.out.println("ID:" + count);
					System.out.println("latitude:" + latitude);
					System.out.println("longtitude:" + longtitude);
					System.out.println("place:" + place);
					System.out.println("name:" + name);
					System.out.println("message:" + message);
					System.out.println("data:" + date);
					System.out.println("-------------8-------------");
				
					insertDB(id, count, name, longtitude, latitude, place, message,date);

					if (++count > 100) {
						twitterStream.shutdown();						
						System.out.println("Information Collection Completed");
					}
			//		count = (count+1) % 101;
				}
			}

			@Override
			public void onDeletionNotice(
					StatusDeletionNotice statusDeletionNotice) {
//				System.out.println("Got a status deletion notice id:"
//						+ statusDeletionNotice.getStatusId());
			}

			@Override
			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
//				System.out.println("Got track limitation notice:"
//						+ numberOfLimitedStatuses);
			}

			@Override
			public void onScrubGeo(long userId, long upToStatusId) {
//				System.out.println("Got scrub_geo event userId:" + userId
//						+ " upToStatusId:" + upToStatusId);
			}

			@Override
			public void onStallWarning(StallWarning warning) {
//				System.out.println("Got stall warning:" + warning);
			}

			@Override
			public void onException(Exception ex) {
				ex.printStackTrace();
			}
		};
		twitterStream.addListener(listener);
		twitterStream.sample();
	}

	private static void insertDB(String id, int count, String name,
			double longtitude, double latitude, String place, String message,String date) {
		// Add an item
		Map<String, AttributeValue> item = newItem(id, count, name, longtitude,
				latitude, place, message, date);
//		System.out.println(item);
		PutItemRequest putItemRequest = new PutItemRequest(tableName, item);
		PutItemResult putItemResult = dynamoDB.putItem(putItemRequest);
//		System.out.println("Result: " + putItemResult);
	}

	private static Map<String, AttributeValue> newItem(String id, int count,
			String name, double longtitude, double latitude, String place,
			String message, String date) {
		Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
		item.put("id", new AttributeValue(id));
		item.put("count", new AttributeValue().withN(Integer.toString(count)));
		item.put("name", new AttributeValue(name));
		item.put("longtitude",
				new AttributeValue().withN(Double.toString(longtitude)));
		item.put("latitude",
				new AttributeValue().withN(Double.toString(latitude)));
		item.put("place", new AttributeValue(place));
		item.put("message", new AttributeValue(message));
		item.put("date", new AttributeValue(date));

		return item;
	}

	
	
	
	

}
