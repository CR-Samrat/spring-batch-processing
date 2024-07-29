package com.example.spring_batch_demo.controller;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/students")
public class StudentController {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private Job job;
    
    @PostMapping("/exportData")
    public ResponseEntity<?> exportCsvToDatabase(){
        JobParameters jobParameters = new JobParametersBuilder()
                                            .addLong("startAt", System.currentTimeMillis())
                                            .toJobParameters();

        try {
            JobExecution jobExecution = jobLauncher.run(job, jobParameters);

            if(!jobExecution.getStatus().isUnsuccessful()){
                return new ResponseEntity<>("File exported successfully !!", HttpStatus.ACCEPTED);
            }
        } catch (JobExecutionAlreadyRunningException | JobRestartException | JobInstanceAlreadyCompleteException
                | JobParametersInvalidException e) {
            e.printStackTrace();
        }

        return new ResponseEntity<>("File export unsuccessful !!", HttpStatus.INTERNAL_SERVER_ERROR);
    }
}
