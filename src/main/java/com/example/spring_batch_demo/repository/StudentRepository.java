package com.example.spring_batch_demo.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.example.spring_batch_demo.model.Student;

public interface StudentRepository extends JpaRepository<Student, Integer>{
    
}
