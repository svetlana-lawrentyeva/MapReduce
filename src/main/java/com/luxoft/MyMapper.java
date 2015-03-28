package com.luxoft;


import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Iterator;

/**
 * Created by Svetlana Lawrentyeva on 27.03.15.
 */
public class MyMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    private static Iterable<String> HEADERS;
    private static String ROOT_NAME;

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        XMLOutputFactory output = XMLOutputFactory.newInstance();
        Iterable<String> datas = Splitter.on(",").trimResults().split(value.toString());
        try {
            StringWriter stringWriter = new StringWriter();
            XMLStreamWriter writer = output.createXMLStreamWriter(stringWriter);
            writer.writeStartDocument();
            writer.writeStartElement(ROOT_NAME);

            Iterator<String> headerIterator = HEADERS.iterator();
            Iterator<String> dataIterator = datas.iterator();
            while(headerIterator.hasNext() && dataIterator.hasNext()) {
                writer.writeStartElement(headerIterator.next());
                writer.writeCharacters(dataIterator.next());
                writer.writeEndElement();
            }

            writer.writeEndElement();
            writer.flush();
            writer.close();

            System.out.println("MAPPER:");
            System.out.println(stringWriter.toString());

            context.write(key, new Text(stringWriter.toString()));
            stringWriter.close();
        } catch (XMLStreamException e) {
            e.printStackTrace();
        }
    }

    public static void createHeader(Iterable<String> headers) {
        HEADERS = Iterables.unmodifiableIterable(headers);
    }
    public static void setRootName(String rootName){
        ROOT_NAME = rootName;
    }
}
