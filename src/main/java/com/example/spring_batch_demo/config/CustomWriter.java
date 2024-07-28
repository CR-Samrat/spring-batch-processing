package com.example.spring_batch_demo.config;

import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.example.spring_batch_demo.model.Student;
import com.example.spring_batch_demo.repository.StudentRepository;

@Component
public class CustomWriter implements ItemWriter<Student>{

    @Autowired
    private StudentRepository studentRepository;

    @Override
    public void write(Chunk<? extends Student> chunk) throws Exception {
        System.out.println("Thread name : "+Thread.currentThread().getName());
        this.studentRepository.saveAll(chunk.getItems());
    }
    
}
