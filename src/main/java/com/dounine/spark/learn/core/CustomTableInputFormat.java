package com.dounine.spark.learn.core;

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

import java.io.IOException;
import java.util.List;

public class CustomTableInputFormat extends TableInputFormat {

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        return super.getSplits(context);
    }

    @Override
    public void setScan(Scan scan) {
        RegexStringComparator rc = new RegexStringComparator(".*20180611.*");
        RowFilter rf = new RowFilter(CompareOperator.EQUAL, rc);
        scan.setFilter(rf);
        super.setScan(scan);
    }
}
