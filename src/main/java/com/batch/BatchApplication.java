package com.batch;

import com.batch.models.Order;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.FileSystemResource;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import javax.sql.DataSource;
import java.util.Date;
import java.util.List;


@SpringBootApplication
@EnableBatchProcessing
@EnableScheduling
@Slf4j
public class BatchApplication {

	public static String[] tokens = new String[] {"order_id", "first_name", "last_name", "email", "cost", "item_id", "item_name", "ship_date"};
	public static String[] names = new String[] {"orderId", "firstName", "lastName", "email", "cost", "itemId", "itemName", "shipDate"};
	public static String ORDER_SQL = "select order_id, first_name, last_name, email, cost, item_id, item_name, ship_date from SHIPPED_ORDER order by order_id";
	public static String INSERT_SQL = "insert into SHIPPED_ORDER_OUTPUT (order_id, first_name, last_name, email, cost, item_id, item_name, ship_date)"
			+ "values(?,?,?,?,?,?,?,?)";

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	JobLauncher jobLauncher;

	@Autowired
	public DataSource dataSource;

	@Scheduled(cron = "0/30 * * * * *")
	public void runJob() throws Exception {
		JobParametersBuilder parametersBuilder = new JobParametersBuilder();
		parametersBuilder.addDate("runTime", new Date());
		this.jobLauncher.run(job(), parametersBuilder.toJobParameters());
	}

//	//reading data from csv
//	@Bean
//	public ItemReader<Order> itemReader() {
//		FlatFileItemReader itemReader = new FlatFileItemReader<Order>();
//		itemReader.setLinesToSkip(1);
//		itemReader.setResource(new FileSystemResource("C:\\Users\\Abdim\\Desktop\\JAVA\\batch-application\\batch-application\\src\\main\\data\\shipped_orders.csv"));
//
//		DefaultLineMapper<Order> lineMapper = new DefaultLineMapper<Order>();
//		DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
//		tokenizer.setNames(tokens);
//
//		//set tokenizer on line mapper
//		lineMapper.setLineTokenizer(tokenizer);
//
//		lineMapper.setFieldSetMapper(new OrderFieldSetMapper());
//
//		itemReader.setLineMapper(lineMapper);
//		return itemReader;
//	}

//	@Bean
//	public ItemWriter<Order> itemWriter() {
//		FlatFileItemWriter<Order> itemWriter = new FlatFileItemWriter<Order>();
//		itemWriter.setResource(new FileSystemResource("C:\\Users\\Abdim\\Desktop\\JAVA\\batch-application\\batch-application\\src\\main\\data\\orders_from_writer.csv"));
//
//		DelimitedLineAggregator<Order> aggregator = new DelimitedLineAggregator<Order>();
//		aggregator.setDelimiter(",");
//
//		BeanWrapperFieldExtractor<Order> fieldExtractor = new BeanWrapperFieldExtractor<Order>();
//		fieldExtractor.setNames(names);
//		aggregator.setFieldExtractor(fieldExtractor);
//
//		itemWriter.setLineAggregator(aggregator);
//		return itemWriter;
//	}

	@Bean
	public ItemWriter<Order> itemWriter() {
		return new JdbcBatchItemWriterBuilder<Order>()
				.dataSource(dataSource)
				.sql(INSERT_SQL)
				.itemPreparedStatementSetter(new OrderItemPreparedStatement())
				.build();
	}

	@Bean
	public PagingQueryProvider queryProvider() throws Exception {
		SqlPagingQueryProviderFactoryBean factoryBean = new SqlPagingQueryProviderFactoryBean();
		factoryBean.setSelectClause("select order_id, first_name, last_name, email, cost, item_id, item_name, ship_date");
		factoryBean.setFromClause("from SHIPPED_ORDER");
		factoryBean.setSortKey("order_id");
		factoryBean.setDataSource(dataSource);
		return factoryBean.getObject();
	}

	@Bean
	public ItemReader<Order> itemReader() throws Exception {
		return new JdbcPagingItemReaderBuilder<Order>()
				.dataSource(dataSource)
				.name("jdbcCursorItemReader")
				.queryProvider(queryProvider())
				.rowMapper(new OrderRowMapper())
				.build();
	}

	@Bean
	public Step chunkBasedStep() throws Exception {
		return this.stepBuilderFactory.get("chunkBasedStep")
				.<Order, Order>chunk(3)
				.reader(itemReader())
				.writer(itemWriter()).build();
	}

	@Bean
	public Job job () throws Exception {
		return this.jobBuilderFactory.get("job")
				.start(chunkBasedStep())
				.build();
	}

	public static void main(String[] args) {
		SpringApplication.run(BatchApplication.class, args);
	}

}
