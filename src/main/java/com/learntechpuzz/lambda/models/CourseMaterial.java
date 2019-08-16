package com.learntechpuzz.lambda.models;

import java.io.Serializable;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

@DynamoDBTable(tableName = "CourseMaterials")
public class CourseMaterial implements Serializable {

	private static final long serialVersionUID = 1L;

	@DynamoDBHashKey(attributeName = "ID")
	private int id;

	@DynamoDBRangeKey(attributeName = "CourseID")
	private int courseId;

	@DynamoDBAttribute(attributeName = "Tag")
	private String tag;

	@DynamoDBAttribute(attributeName = "FileName")
	private String fileName;

	public CourseMaterial() {
		super();
	}

	public CourseMaterial(int id, int courseId, String tag, String fileName) {
		super();
		this.id = id;
		this.courseId = courseId;
		this.tag = tag;
		this.fileName = fileName;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public int getCourseId() {
		return courseId;
	}

	public void setCourseId(int courseId) {
		this.courseId = courseId;
	}

	public String getTag() {
		return tag;
	}

	public void setTag(String tag) {
		this.tag = tag;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	@Override
	public String toString() {
		return "CourseMaterials [id=" + id + ", courseId=" + courseId + ", tag=" + tag + ", fileName=" + fileName + "]";
	}

}
