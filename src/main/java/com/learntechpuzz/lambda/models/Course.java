package com.learntechpuzz.lambda.models;

import com.opencsv.bean.CsvBindByName;

public class Course {

	@CsvBindByName(column = "CourseID", required = true)
	private int courseId;

	@CsvBindByName(column = "Title", required = true)
	private String title;

	@CsvBindByName(column = "Summary", required = true)
	private String summary;

	@CsvBindByName(column = "Logo_FileName", required = true)
	private String logoFileName;

	@CsvBindByName(column = "About", required = true)
	private String about;
	
	@CsvBindByName(column = "CourseContents_FileName", required = true)
	private String courseContentsFileName;

	public int getCourseId() {
		return courseId;
	}

	public void setCourseId(int courseId) {
		this.courseId = courseId;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getSummary() {
		return summary;
	}

	public void setSummary(String summary) {
		this.summary = summary;
	}

	public String getLogoFileName() {
		return logoFileName;
	}

	public void setLogoFileName(String logoFileName) {
		this.logoFileName = logoFileName;
	}

	public String getAbout() {
		return about;
	}

	public void setAbout(String about) {
		this.about = about;
	}

	public String getCourseContentsFileName() {
		return courseContentsFileName;
	}

	public void setCourseContentsFileName(String courseContentsFileName) {
		this.courseContentsFileName = courseContentsFileName;
	}

	@Override
	public String toString() {
		return "Course [courseId=" + courseId + ", title=" + title + ", summary=" + summary + ", logoFileName="
				+ logoFileName + ", about=" + about + ", courseContentsFileName=" + courseContentsFileName + "]";
	}
	
	
}
