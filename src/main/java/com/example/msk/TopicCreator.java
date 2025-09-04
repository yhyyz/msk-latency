package com.example.msk;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class TopicCreator {
    
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: java TopicCreator <bootstrap-servers> <topic-name> [partitions] [replication-factor]");
            System.exit(1);
        }
        
        String bootstrapServers = args[0];
        String topicName = args[1];
        int partitions = args.length > 2 ? Integer.parseInt(args[2]) : 16;
        short replicationFactor = args.length > 3 ? Short.parseShort(args[3]) : (short) 3;
        
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        
        try (AdminClient adminClient = AdminClient.create(props)) {
            NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);
            CreateTopicsResult result = adminClient.createTopics(Collections.singletonList(newTopic));
            result.all().get();
            System.out.println("Topic '" + topicName + "' created successfully with " + partitions + " partitions and replication factor " + replicationFactor);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                System.out.println("Topic '" + topicName + "' already exists");
            } else {
                System.err.println("Failed to create topic: " + e.getMessage());
                System.exit(1);
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            System.exit(1);
        }
    }
}
