package com.example;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;

public class KakfaAvroConsumer {
    public static void main(String[] args) throws IOException {
        String topicName = "DataProducer";
        String groupName = "DataProducerTopicGroup";

        Properties props = new Properties();
        InputStream propFile = null;
        KafkaConsumer<String, GenericRecord> consumer = null;

        try {
            propFile = new FileInputStream("src/main/resources/consumer-default.properties");
            props.load(propFile);
            props.put("group.id", groupName);

            consumer = new KafkaConsumer<String, GenericRecord>(props);
            consumer.subscribe(Arrays.asList(topicName));

            while (true) {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(100);
                //System.out.println("Records size = " + records.count());
                for (ConsumerRecord<String, GenericRecord> record : records) {
                    System.out.println("Record= " + record.value());
                    insertDataToTarget(record);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            propFile.close();
            consumer.close();
        }
    }

    private static void insertDataToTarget(ConsumerRecord<String, GenericRecord> record) throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/cotiviti", "root",
                "cloudera");
        String insertQuery = "insert into ss_225_elig values(?,?,?,?,?,?,?,?,?,?)";
        PreparedStatement preparedStatement = connection.prepareStatement(insertQuery);
        //preparedStatement.setInt(1, record.getSn());
        preparedStatement.setString(1, record.value().get("enrid").toString());
        preparedStatement.setString(2, record.value().get("memberfirstname").toString());
        preparedStatement.setString(3, record.value().get("memberlastname").toString());
        preparedStatement.setString(4, record.value().get("relflag").toString());
        preparedStatement.setString(5, record.value().get("gender").toString());
        //preparedStatement.setDate(7, record.getDob());
        preparedStatement.setString(6, record.value().get("address").toString());
        preparedStatement.setString(7, record.value().get("city").toString());
        preparedStatement.setString(8, record.value().get("state").toString());
        preparedStatement.setString(9, record.value().get("zip").toString());
        preparedStatement.setString(10, record.value().get("phonenumber").toString());
        //preparedStatement.setDate(13, record.getEffdate());
        //preparedStatement.setDate(2, record.getTermdate());

        preparedStatement.executeUpdate();

        connection.close();
    }
}
