package com.learntechpuzz.lambda.handlers;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Iterator;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.learntechpuzz.lambda.dynamodb.DynamoDBManager;
import com.learntechpuzz.lambda.models.Course;
import com.learntechpuzz.lambda.models.CourseMaterial;
import com.learntechpuzz.lambda.models.CsvCourse;
import com.learntechpuzz.lambda.models.CsvCourseMaterial;
import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;

public class ApplicationDataLoaderHandler implements RequestHandler<S3Event, String> {
	private AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();
	private static final DynamoDBMapper mapper = DynamoDBManager.mapper();

	public ApplicationDataLoaderHandler() {
	}

	public String loadData(S3Event event, Context context) {
		context.getLogger().log("Received event: " + event);

		String bucket = event.getRecords().get(0).getS3().getBucket().getName();
		String key = event.getRecords().get(0).getS3().getObject().getKey();

		S3Object response = s3.getObject(new GetObjectRequest(bucket, key));
		String result = "Saved";

		BufferedReader br = new BufferedReader(new InputStreamReader(response.getObjectContent()));

		if (key.equalsIgnoreCase("Courses.csv")) {
			CsvToBean<CsvCourse> coursesBean = new CsvToBeanBuilder<CsvCourse>(br).withType(CsvCourse.class)
					.withIgnoreLeadingWhiteSpace(true).build();

			Iterator<CsvCourse> courses = coursesBean.iterator();

			while (courses.hasNext()) {
				CsvCourse course = courses.next();
				context.getLogger().log(course.toString());
				saveCourses(course.getCourseId(), course.getTitle(), course.getSummary(), course.getLogoFileName(),
						course.getAbout(), course.getCourseContentsFileName());

			}
		}

		if (key.equalsIgnoreCase("CourseMaterials.csv")) {
			CsvToBean<CsvCourseMaterial> courseMaterialsBean = new CsvToBeanBuilder<CsvCourseMaterial>(br)
					.withType(CsvCourseMaterial.class).withIgnoreLeadingWhiteSpace(true).build();

			Iterator<CsvCourseMaterial> courseMaterials = courseMaterialsBean.iterator();

			while (courseMaterials.hasNext()) {
				CsvCourseMaterial courseMaterial = courseMaterials.next();
				context.getLogger().log(courseMaterials.toString());
				saveCourseMaterials(courseMaterial.getId(), courseMaterial.getCourseId(), courseMaterial.getTag(),
						courseMaterial.getFileName());

			}
		}

		return result;

	}

	private void saveCourseMaterials(int id, int courseId, String tag, String fileName) {
		try {
			mapper.save(new CourseMaterial(id, courseId, tag, fileName));
		} catch (Exception e) {
			System.err.println("saveCourseMaterials failed.");
			System.err.println(e.getMessage());

		}
	}

	private void saveCourses(int courseId, String title, String summary, String logoFileName, String about,
			String courseContentsFileName) {
		try {
			mapper.save(new Course(courseId, title, summary, logoFileName, about, courseContentsFileName));
		} catch (Exception e) {
			System.err.println("saveCourses failed.");
			System.err.println(e.getMessage());

		}
	}

	@Override
	public String handleRequest(S3Event input, Context context) {
		return null;
	}
}