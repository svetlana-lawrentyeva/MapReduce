package com.example;

import junit.framework.TestCase;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.*;

public class ExampleRunner extends TestCase {

    MapDriver<Text, Text, Text, Text> mapDriver;
    ReduceDriver<Text, Text, Text, Text> reduceDriver;
    MapReduceDriver<Text, Text, Text, Text, Text, Text> mapReduceDriver;

    @Before
    public void setUp() {
        WordMapper mapper = new WordMapper();
        AllTranslationsReducer reducer = new AllTranslationsReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }


    public void testMapper() {
        List<File> files = new ArrayList();
        files.add(new File("rus.txt"));
        files.add(new File("az.txt"));
        files.add(new File("ukr.txt"));

        for(File file:files){
            Set<Map.Entry<Text, Text>> values = getMapValue(file, ",").entrySet();
            for(Map.Entry<Text, Text> entry:values){
                mapDriver.withInput(entry.getKey(), entry.getValue());
            }
        }
    }

    private Map<Text, Text> getMapValue(File f, String separator){
        Map<Text, Text>resultMap = new TreeMap<Text, Text>();
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
            String line;
            while((line=reader.readLine())!=null){
                resultMap.put(new Text(line.substring(0,line.indexOf(separator))),
                        new Text(line.substring(line.indexOf(separator))));
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return resultMap;
    }


    @Test
    public void testMapReduce() {
        List<File> files = new ArrayList();
        files.add(new File("rus.txt"));
        files.add(new File("az.txt"));
        files.add(new File("ukr.txt"));

        for(File file:files){
            Set<Map.Entry<Text, Text>> values = getMapValue(file, ",").entrySet();
            for(Map.Entry<Text, Text> entry:values){
                mapReduceDriver.withInput(entry.getKey(), entry.getValue());
            }
        }

        File resultFile = new File("resultWord.txt");
        Set<Map.Entry<Text, Text>> values = getMapValue(resultFile, "|").entrySet();
        for(Map.Entry<Text, Text> entry:values){
            mapReduceDriver.withOutput(entry.getKey(), entry.getValue());
        }
        mapReduceDriver.runTest();
    }
}