package com.batch;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.text.ParseException;

@SpringBootApplication
@EnableBatchProcessing
@Slf4j
public class BatchApplication {

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Bean
	public JobExecutionDecider decider() {
		return new DeliveryDecider();
	}

	@Bean
	public StepExecutionListener selectFlowerListener() {
		return new FlowersSelectionStepExecutionListener();
	}

	@Bean
	public Step nestedBillingJobStep() {
		return stepBuilderFactory.get("nestedBillingStep").job(billingJob()).build();
	}

	@Bean
	public Step sendInvoiceStep() {
		return stepBuilderFactory.get("sendInvoiceStep").tasklet(new Tasklet() {
			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				System.out.println("Send invoice to customer step");
				return RepeatStatus.FINISHED;
			}
		}).build();
	}

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
	public Step leavePackageAtDoor() {
		return stepBuilderFactory.get("leavePackageAtDoor").tasklet(new Tasklet() {
			@Override
			public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
				System.out.println("Leaving package at door for customer........");
				return RepeatStatus.FINISHED;
			}
		}).build();
	}

	@Bean
	public Step driverGotLost() {
		return stepBuilderFactory.get("driverGotLost").tasklet(new Tasklet() {
			@Override
			public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
				System.out.println("Driver got lost. Retrying again !!");
				return RepeatStatus.FINISHED;
			}
		}).build();
	}

	@Bean
	public Step deliverPackage() {
		return stepBuilderFactory.get("deliverPackage").tasklet(new Tasklet() {
			@Override
			public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
				System.out.println("Delivering item to customer");
				return RepeatStatus.FINISHED;
			}
		}).build();
	}

	@Bean
	public Step driveToLocation() {
		boolean GOT_LOST = false;
		return stepBuilderFactory.get("driveToLocation").tasklet(new Tasklet() {
			@Override
			public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
				if (GOT_LOST) {
					throw new RuntimeException("Driver got lost");
				}
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
//				String item = chunkContext.getStepContext().getJobParameters().get("item").toString();
//				String date = chunkContext.getStepContext().getJobParameters().get("run.date").toString();
				System.out.println(String.format("The item has been packaged ********** "));
				return RepeatStatus.FINISHED;
			}
		}).build();
	}

	/* START OF FLOWERS JOB*/
	//flower delivery job steps

	@Bean
	public Step selectFlowersStep() {
		return stepBuilderFactory.get("selectFlowersStep").tasklet(new Tasklet() {
			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				System.out.println("Arranging flowers for delivery");
				return RepeatStatus.FINISHED;
			}
		}).listener(selectFlowerListener()).build();
	}

	@Bean
	public Step arrangeFlowersStep() {
		return stepBuilderFactory.get("arrangeFlowersStep").tasklet(new Tasklet() {
			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				System.out.println("Arranging flowers ****");
				return RepeatStatus.FINISHED;
			}
		}).build();
	}

	@Bean
	public Step removeThornsStep() {
		return stepBuilderFactory.get("removeThornsStep").tasklet(new Tasklet() {
			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				System.out.println("Removing thorns from flowers");
				return RepeatStatus.FINISHED;
			}
		}).build();
	}

	@Bean
	public Job prepareFlowers() {
		return jobBuilderFactory.get("prepareFlowersJob")
				.start(selectFlowersStep())
					.on("TRIM_REQUIRED").to(removeThornsStep()).next(arrangeFlowersStep())
				.from(selectFlowersStep())
					.on("NO_TRIM_REQUIRED").to(arrangeFlowersStep())
				.end()
				.build();
	}

	/* END OF FLOWERS JOB*/


	@Bean
	public Job deliverPackageJob() throws ParseException {
		return jobBuilderFactory.get("deliverPackageJob")
				.start(packageItemStep())
				.next(driveToLocation())
					.on("FAILED").to(driverGotLost())
				.from(driveToLocation())
					.on("*").to(decider())
						.on("PRESENT").to(itemDelivered())
					.from(decider())
						.on("NOT_PRESENT").to(leavePackageAtDoor())
				.next(nestedBillingJobStep())
				.end()
				.build();
	}

	@Bean
	public Job billingJob(){
		return jobBuilderFactory.get("billingJob").start(sendInvoiceStep()).build();
	}



	public static void main(String[] args) {
		SpringApplication.run(BatchApplication.class, args);
	}

}
