package com.example.spring_batch_demo.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "student_performance")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Student {

    @Id
    @Column(name="student_id")
    private int id;
    
    @Column(name = "gender")
    private String gender;

    @Column(name = "ethnicity")
    private String ethnicity;

    @Column(name = "parental_education")
    private String parentalEducation;

    @Column(name = "lunch")
    private String lunch;

    @Column(name = "course_status")
    private String course;

    @Column(name = "math_score")
    private int mathScore;

    @Column(name = "reading_score")
    private int readingScore;

    @Column(name = "writing_score")
    private int writingScore;
}
