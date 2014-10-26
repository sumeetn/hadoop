package com.smn.hadoop.rin;

import java.io.IOException;
import java.io.StringReader;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;


public class ObjectKeyMapper extends
		Mapper<LongWritable, Text, IntWritable, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();

		String eventOrObjectFk = getId(line);

		try {
			int id = Integer.parseInt(eventOrObjectFk);
			//System.out.println("id -> "+id+"   line -> "+line);
			context.write(new IntWritable(id), new Text(line));
		} catch (Exception ex) {

		}
	}

	private String getId(String line) {
		String id = null;
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder;
		try {
			builder = factory.newDocumentBuilder();
			Document doc = null;
			doc = builder.parse(new InputSource(new StringReader(line)));
			Node xmlNode = doc.getFirstChild();
			String nodeName = xmlNode.getNodeName();
			if (nodeName.equalsIgnoreCase("event")) {
				id = xmlNode.getAttributes().getNamedItem("id").getNodeValue();
			} else {
				id = xmlNode.getAttributes().getNamedItem("objectFK") == null ? xmlNode
						.getAttributes().getNamedItem("eventFK").getNodeValue()
						: xmlNode.getAttributes().getNamedItem("objectFK")
								.getNodeValue();
			}
		} catch (Exception e) {
			id = null;
		}

		return id;
	}

	public static void main(String[] args) {
		String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><event id=\"1821969\" name=\"Viktor Troicki-Juergen Zopp\" tournament_stageFK=\"835830\" startdate=\"2014-08-30 12:00:00\" eventstatusFK=\"0\" status_type=\"notstarted\" status_descFK=\"1\" enetID=\"0\" enetSportID=\"\" n=\"0\" ut=\"2014-08-29 19:22:52\" del=\"no\" locked=\"none\" />";
		String eventFk = "<event_incident comment=\"''\" del=\"no\" elapsed=\"0\" elapsed_plus=\"0\" enetID=\"0\" eventFK=\"1821969\" event_incident_typeFK=\"670\" id=\"3677169\" n=\"0\" sortorder=\"1\" sportFK=\"2\" ut=\"2014-08-30 17:10:46\"/>";
		String objectFk = "<property del=\"no\" id=\"21068449\" n=\"2\" name=\"Comment\" object=\"event\" objectFK=\"1821969\" type=\"metadata\" ut=\"2014-08-30 17:07:49\" value=\"None\"/>";
		ObjectKeyMapper obj = new ObjectKeyMapper();
		System.out.println(obj.getId(objectFk));

	}

}
