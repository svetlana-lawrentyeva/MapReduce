package com.luxoft;

import com.google.common.base.Splitter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import javax.xml.transform.*;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Iterator;

/**
 * Created by Svetlana Lawrentyeva on 28.03.15.
 */
public class MyReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

    @Override
    public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text value:values) {
            try {
                TransformerFactory factory = TransformerFactory.newInstance();
                Source xslt = new StreamSource(new File("tr.xsl"));
                Transformer transformer = factory.newTransformer(xslt);

                StringReader reader = new StringReader(value.toString());
                Source text = new StreamSource(reader);
                StringWriter writer = new StringWriter();
                transformer.transform(text, new StreamResult(writer));

                System.out.println("REDUCER:");
                System.out.println(writer);

                writer.flush();
                context.write(key, new Text(writer.toString()));
                writer.close();
            } catch (TransformerConfigurationException e) {
                e.printStackTrace();
            } catch (TransformerException e) {
                e.printStackTrace();
            }
        }
    }
}
