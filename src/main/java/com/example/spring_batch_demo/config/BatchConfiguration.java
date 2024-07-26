package com.example.spring_batch_demo.config;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.batch.core.Job;

import com.example.spring_batch_demo.model.Student;
import com.example.spring_batch_demo.repository.StudentRepository;

@Configuration
public class BatchConfiguration {
    
    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Autowired
    private StudentRepository studentRepository;

    // Observe the Spring-Batch-Architecture.jpg
    // Process from right to left
    // First create Item reader, Item processor & Item writer, Only after this create Step then Job

    // ItemReader specifies what things need to remember when reading from source file (like source address, etc)
    @Bean
    protected FlatFileItemReader<Student> reader(){
        FlatFileItemReader<Student> itemReader = new FlatFileItemReader<>();

        itemReader.setResource(new FileSystemResource("src/main/resources/Students_Performance.csv"));
        itemReader.setName("csvReader");
        itemReader.setLinesToSkip(1);
        itemReader.setLineMapper(lineMapper());

        return itemReader;
    }

    public LineMapper<Student> lineMapper(){
        DefaultLineMapper<Student> mapper = new DefaultLineMapper<>();

        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames("id","gender","ethnicity","parentalEducation","lunch",
                                        "course","mathScore","readingScore","writingScore");

        BeanWrapperFieldSetMapper<Student> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Student.class);

        mapper.setLineTokenizer(lineTokenizer);
        mapper.setFieldSetMapper(fieldSetMapper);

        return mapper;
    }

    // ItemProcessor specifies what field to include and what not to include in the target file
    @Bean
    protected CustomProcessor processor(){
        return new CustomProcessor();
    }

    // ItemWriter specifies how to write in the target file (Ex - write using repository)
    @Bean
    protected RepositoryItemWriter<Student> writer(){
        RepositoryItemWriter<Student> itemWriter = new RepositoryItemWriter<>();

        itemWriter.setRepository(studentRepository);
        itemWriter.setMethodName("save");

        return itemWriter;
    }

    // Step is the combination of ItemReader, ItemProcessor & ItemWriter. Spring can have multiple Steps
    @Bean
    protected Step step1(){
        return new StepBuilder("step1", jobRepository)
                    .<Student, Student>chunk(10, transactionManager)
                    .reader(reader())
                    .processor(processor())
                    .writer(writer())
                    .taskExecutor(taskExecutor())
                    .build();
    }

    // Job is the final step where is task is actually takes place by feeding each Step into Job
    @Bean
    protected Job runJob(){
        return new JobBuilder("myJob", jobRepository)
                    .start(step1())
                    .build();
    }

    // Load data in async mode to speed up the process but removes the ordering (Optional)
    @Bean
    protected TaskExecutor taskExecutor(){
        SimpleAsyncTaskExecutor asyncExecutor = new SimpleAsyncTaskExecutor();
        asyncExecutor.setConcurrencyLimit(10);

        return asyncExecutor;
    }
}
