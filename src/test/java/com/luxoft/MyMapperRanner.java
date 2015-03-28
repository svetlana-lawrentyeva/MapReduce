package com.luxoft;

import com.google.common.base.Splitter;
import junit.framework.TestCase;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.io.*;

public class MyMapperRanner extends TestCase {

    MapDriver<LongWritable, Text, LongWritable, Text> mapDriver;
    ReduceDriver<LongWritable, Text, LongWritable, Text> reduceDriver;
    MapReduceDriver<LongWritable, Text, LongWritable, Text, LongWritable, Text> mapReduceDriver;

    @Before
    public void setUp() {
        MyMapper mapper = new MyMapper();
        MyReducer reducer = new MyReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() {
        File f = new File("excange.csv");
        MyMapper.setRootName("excange");
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
            String line;
            while((line = reader.readLine()) != null){
                if(line.substring(0, line.indexOf(",")).equals("pk")){
                    MyMapper.createHeader(Splitter.on(",").trimResults().split(line));
                } else {
                    mapReduceDriver.withInput(new LongWritable(Long.parseLong(line.substring(0, line.indexOf(",")))), new Text(line));
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        File r = new File("results.txt");
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(r)));
            String line;
            long counter = 1;
            while((line = reader.readLine()) != null){
                mapReduceDriver.withOutput(new LongWritable(counter++), new Text(line));
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        mapReduceDriver.runTest(false);
    }

}