package com.batch;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SimpleItemReader implements ItemReader<String> {

    private List<String> data = new ArrayList<>();
    private Iterator<String> iterator;

    public SimpleItemReader() {
        this.data.add("Item One");
        this.data.add("Item Two");
        this.data.add("Item Three");
        this.data.add("Item Four");
        this.data.add("Item Five");
        this.iterator = this.data.iterator();
    }

    @Override
    public String read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        return iterator.hasNext() ? iterator.next() : null;
    }
}
