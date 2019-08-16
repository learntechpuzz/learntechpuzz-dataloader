package com.learntechpuzz.lambda.handlers;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.io.IOUtils;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.lambda.runtime.Context;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.learntechpuzz.lambda.dynamodb.DynamoDBManager;
import com.learntechpuzz.lambda.models.Course;
import com.learntechpuzz.lambda.models.CourseMaterial;

public class CoursesHandler {

	private static final DynamoDBMapper mapper = DynamoDBManager.mapper();

	public void getAllCourses(InputStream request, OutputStream response, Context context) {
		context.getLogger().log("\nCalling getAllEventsHandler function");
		List<Course> courses = mapper.scan(Course.class, new DynamoDBScanExpression());
		context.getLogger().log("\ncourses:" + courses);
		output(courses, response, context);
	}

	public void findCourseByID(InputStream request, OutputStream response, Context context) {
		context.getLogger().log("\nCalling findCourseByID function");
		int courseId = 1;
		Course course = mapper.load(Course.class, courseId);
		context.getLogger().log("\ncourse:" + course);
		output(Optional.ofNullable(course), response, context);
	}

	public void findCourseMaterialsByCourseID(InputStream request, OutputStream response, Context context) {
		context.getLogger().log("\nCalling findCourseMaterialsByCourseID function");
		String courseId = "1";
		Map<String, AttributeValue> eav = new HashMap<>();
		eav.put(":courseId", new AttributeValue().withN(courseId));

		DynamoDBScanExpression scanExpression = new DynamoDBScanExpression()
				.withFilterExpression("CourseID = :courseId").withExpressionAttributeValues(eav);
		List<CourseMaterial> courseMaterials = mapper.scan(CourseMaterial.class, scanExpression);
        List<CourseMaterial> sortedCourseMaterials = new LinkedList<>();
        sortedCourseMaterials.addAll(courseMaterials);
        sortedCourseMaterials.sort( (e1, e2) -> e1.getId() <= e2.getId() ? -1 : 1 );
		output(sortedCourseMaterials, response, context);
	}

	public void findCourseMaterialsByCourseIDAndTag(InputStream request, OutputStream response, Context context) {
		context.getLogger().log("\nCalling findCourseMaterialsByCourseIDAndTag function");
		String courseId = "1";
		String tag = "EC2";
		Map<String, AttributeValue> eav = new HashMap<>();
		eav.put(":courseId", new AttributeValue().withN(courseId));
		eav.put(":tag", new AttributeValue().withS(tag));

		DynamoDBScanExpression scanExpression = new DynamoDBScanExpression()
				.withFilterExpression("CourseID = :courseId and contains(Tag, :tag)").withExpressionAttributeValues(eav);
		List<CourseMaterial> courseMaterials = mapper.scan(CourseMaterial.class, scanExpression);
		output(courseMaterials, response, context);
	}

	protected Gson getGson() {
		return new GsonBuilder().setPrettyPrinting().create();
	}

	protected void output(Object out, OutputStream response, Context context) {
		String output = getGson().toJson(out);
		context.getLogger().log("\noutput: " + output);
		try {
			IOUtils.write(output, response, "UTF-8");
		} catch (final IOException e) {
			context.getLogger().log("\nError while writing response" + e.getMessage());
		}

	}

}
