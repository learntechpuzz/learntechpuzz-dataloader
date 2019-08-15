package com.learntechpuzz.lambda.handlers;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Iterator;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.learntechpuzz.lambda.models.Course;
import com.learntechpuzz.lambda.models.CourseMaterial;
import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;

public class ApplicationDataLoaderHandler implements RequestHandler<S3Event, String> {
	private AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();
	private AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
	private DynamoDB dynamoDB = new DynamoDB(client);

	private static String tableName = "Courses";

	public ApplicationDataLoaderHandler() {
	}

	// Test purpose only.
	ApplicationDataLoaderHandler(AmazonS3 s3) {
		this.s3 = s3;
	}

	@Override
	public String handleRequest(S3Event event, Context context) {
		context.getLogger().log("Received event: " + event);

		// Get the object from the event and show its content type
		String bucket = event.getRecords().get(0).getS3().getBucket().getName();
		String key = event.getRecords().get(0).getS3().getObject().getKey();

		S3Object response = s3.getObject(new GetObjectRequest(bucket, key));
		String contentType = response.getObjectMetadata().getContentType();

		BufferedReader br = new BufferedReader(new InputStreamReader(response.getObjectContent()));

		if (key.equalsIgnoreCase("Courses.csv")) {
			CsvToBean<Course> coursesBean = new CsvToBeanBuilder<Course>(br).withType(Course.class)
					.withIgnoreLeadingWhiteSpace(true).build();

			Iterator<Course> courses = coursesBean.iterator();

			while (courses.hasNext()) {
				Course course = courses.next();
				context.getLogger().log(course.toString());
				addCourses(course.getCourseId(), course.getTitle(), course.getSummary(), course.getLogoFileName(),
						course.getAbout(), course.getCourseContentsFileName());

			}
		}

		if (key.equalsIgnoreCase("CourseMaterials.csv")) {
			CsvToBean<CourseMaterial> courseMaterialsBean = new CsvToBeanBuilder<CourseMaterial>(br)
					.withType(CourseMaterial.class).withIgnoreLeadingWhiteSpace(true).build();

			Iterator<CourseMaterial> courseMaterials = courseMaterialsBean.iterator();

			while (courseMaterials.hasNext()) {
				CourseMaterial courseMaterial = courseMaterials.next();
				context.getLogger().log(courseMaterials.toString());
				addCourseMaterials(courseMaterial.getId(), courseMaterial.getCourseId(), courseMaterial.getTag(),
						courseMaterial.getFileName());

			}
		}

		return contentType;

	}

	private void addCourseMaterials(int id, int courseId, String tag, String fileName) {

		Table table = dynamoDB.getTable("CourseMaterials");
		try {

			Item item = new Item().withPrimaryKey("ID", id).withInt("CourseID", courseId).withString("Tag", tag)
					.withString("fileName", fileName);
			table.putItem(item);

		} catch (Exception e) {
			System.err.println("addCourseMaterials failed.");
			System.err.println(e.getMessage());

		}
	}

	private void addCourses(int courseId, String title, String summary, String logoFileName, String about,
			String courseContentsFileName) {

		Table table = dynamoDB.getTable("Courses");
		try {

			Item item = new Item().withPrimaryKey("CourseID", courseId).withString("Title", title)
					.withString("Summary", summary).withString("LogoFileName", logoFileName).withString("About", about)
					.withString("CourseContentsFileName", courseContentsFileName);
			table.putItem(item);

		} catch (Exception e) {
			System.err.println("addCourses failed.");
			System.err.println(e.getMessage());

		}
	}
}