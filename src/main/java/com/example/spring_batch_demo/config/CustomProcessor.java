package com.example.spring_batch_demo.config;

import org.springframework.batch.item.ItemProcessor;

import com.example.spring_batch_demo.model.Student;

public class CustomProcessor implements ItemProcessor<Student, Student>{

    @Override
    public Student process(Student item) throws Exception {
        return item;
    }
    
}
