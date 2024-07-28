package com.example.spring_batch_demo.config;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
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
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.batch.core.Job;

import com.example.spring_batch_demo.model.Student;
import com.example.spring_batch_demo.partition.ColumnRangePartitioner;

@Configuration
public class BatchConfiguration {
    
    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private PlatformTransactionManager transactionManager;

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

    // ItemWriter specifies how to write in the target file
    @Bean
    protected CustomWriter writer(){
        return new CustomWriter();
    }

    // partitioner is used to partition the whole task into smaller tasks by dividing total number of rows into smaller chunks
    // Each chunk then given to different threads to minimize total time
    @Bean
    protected ColumnRangePartitioner partitioner(){
        return new ColumnRangePartitioner();
    }

    @Bean
    protected PartitionHandler partitionHandler(){
        TaskExecutorPartitionHandler taskExecutorPartitionHandler = new TaskExecutorPartitionHandler();
        taskExecutorPartitionHandler.setGridSize(2); // no. of Threads
        taskExecutorPartitionHandler.setTaskExecutor(taskExecutor());
        taskExecutorPartitionHandler.setStep(slaveStep());

        return taskExecutorPartitionHandler;
    }

    // Step is the combination of ItemReader, ItemProcessor & ItemWriter. Spring can have multiple Steps
    // Each slave that will execute one chunk of (250 or 500) rows
    @Bean
    protected Step slaveStep(){
        return new StepBuilder("slaveStep", jobRepository)
                    // .<Student, Student>chunk(500, transactionManager)
                    .<Student, Student>chunk(10, transactionManager)
                    .reader(reader())
                    .processor(processor())
                    .writer(writer())
                    .taskExecutor(taskExecutor())
                    .build();
    }

    // Master step will manage each slave step
    @Bean
    protected Step masterStep(){
        return new StepBuilder("masterStep", jobRepository)
                    .partitioner(slaveStep().getName(), partitioner())
                    .partitionHandler(partitionHandler())
                    .build();
    }

    // Job is the final step where is task is actually takes place by feeding each Step into Job
    @Bean
    protected Job runJob(){
        return new JobBuilder("myJob", jobRepository)
                    // .flow(masterStep())
                    .flow(slaveStep())
                    .end()
                    .build();
    }

    // Execute the whole task using Threads
    // @Bean
    // protected TaskExecutor taskExecutor(){
    //     ThreadPoolTaskExecutor taskHandler = new ThreadPoolTaskExecutor(); //to execute tasks in Thread manner
    //     taskHandler.setCorePoolSize(4);  // Maximum number of Thread that keep alive at a time
    //     taskHandler.setMaxPoolSize(4);   // Maximum number of Thread that can be generated
    //     taskHandler.setQueueCapacity(4); // If Maximum number of Thread reached then extra will moved to Queue
    //     taskHandler.afterPropertiesSet();

    //     return taskHandler;
    // }

    // Execute the whole task in async manner (Optional)
    @Bean
    protected TaskExecutor taskExecutor(){
        SimpleAsyncTaskExecutor asyncExecutor = new SimpleAsyncTaskExecutor();
        asyncExecutor.setConcurrencyLimit(10);

        return asyncExecutor;
    }
}
