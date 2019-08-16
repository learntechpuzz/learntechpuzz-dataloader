package com.learntechpuzz.lambda.models;

import com.opencsv.bean.CsvBindByName;

public class CsvCourseMaterial {

	@CsvBindByName(column = "ID", required = true)
	private int id;

	@CsvBindByName(column = "CourseID", required = true)
	private int courseId;

	@CsvBindByName(column = "Tag", required = true)
	private String tag;

	@CsvBindByName(column = "FileName", required = true)
	private String fileName;

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
