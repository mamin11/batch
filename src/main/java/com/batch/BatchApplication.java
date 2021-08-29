package com.batch;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
@EnableBatchProcessing
public class BatchApplication {

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Bean
	public Step itemDelivered() {
		return stepBuilderFactory.get("itemDelivered").tasklet(new Tasklet() {
			@Override
			public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
				System.out.println("Item has been delivered to customer");
				return RepeatStatus.FINISHED;
			}
		}).build();
	}

	@Bean
	public Step driveToLocation() {
		return stepBuilderFactory.get("driveToLocation").tasklet(new Tasklet() {
			@Override
			public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
				System.out.println("Driver has arrived at location");
				return RepeatStatus.FINISHED;
			}
		}).build();
	}

	@Bean
	public Step packageItemStep() {
		return stepBuilderFactory.get("packageItemStep").tasklet(new Tasklet() {
			@Override
			public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
				String item = chunkContext.getStepContext().getJobParameters().get("item").toString();
				String date = chunkContext.getStepContext().getJobParameters().get("run.date").toString();
				System.out.println(String.format("The item %s has been packaged at %s", item, date));
				return RepeatStatus.FINISHED;
			}
		}).build();
	}

	@Bean
	public Job deliverPackageJob() {
		return jobBuilderFactory.get("deliverPackage")
				.start(packageItemStep())
				.next(driveToLocation())
				.next(itemDelivered())
				.build();
	}



	public static void main(String[] args) {
		SpringApplication.run(BatchApplication.class, args);
	}

}
