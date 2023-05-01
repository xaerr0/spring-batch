package com.example.batch.demo.config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

    @Bean
    public Job job(JobBuilderFactory jobBuilderFactory, Step nameStep, Step designationStep) {
        return jobBuilderFactory.get("employee-loader-job")
                .incrementer(new RunIdIncrementer())
                .start(nameStep)
                .next(designationStep)
                .build();
    }

    //add your Steps, ItemReaders, ItemProcessors, and ItemWriter below
}