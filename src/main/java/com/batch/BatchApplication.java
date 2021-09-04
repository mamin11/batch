package com.batch;

import com.batch.models.Order;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.FileSystemResource;

import javax.sql.DataSource;
import java.util.List;


@SpringBootApplication
@EnableBatchProcessing
@Slf4j
public class BatchApplication {

	public static String[] tokens = new String[] {"order_id", "first_name", "last_name", "email", "cost", "item_id", "item_name", "ship_date"};
	public static String ORDER_SQL = "select order_id, first_name, last_name, email, cost, item_id, item_name, ship_date from SHIPPED_ORDER order by order_id";

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	public DataSource dataSource;

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
				.writer(new ItemWriter<Order>() {
					@Override
					public void write(List<? extends Order> items) throws Exception {
						System.out.println(String.format("ItemWriter received list of size: %s", items.size()));
						items.forEach(System.out::println);
					}
				}).build();
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
