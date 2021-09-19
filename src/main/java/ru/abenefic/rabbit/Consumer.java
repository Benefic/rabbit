package ru.abenefic.rabbit;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

public class Consumer {

    private static final String EXCHANGE_NAME = "topicsExchanger";
    private static final Scanner scanner = new Scanner(System.in);
    private static final Map<String, String> topicQueues = new HashMap<>();
    private static Channel channel;

    public static void main(String[] args) throws IOException, TimeoutException {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setUsername("admin");
        factory.setPassword("admin");

        Connection connection = factory.newConnection();
        channel = connection.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

        System.out.println("Enter 'set_topic [topic]' to subscribe:");
        while (true) {
            String[] userInput = scanner.nextLine().split(" ");
            if (userInput.length == 1 && userInput[0].equals("exit")) {
                System.exit(0);
            } else if (userInput.length == 2 && userInput[0].equals("set_topic")) {
                registerTopic(userInput[1]);
            } else if (userInput.length == 2 && userInput[0].equals("exit_topic")) {
                leaveTopic(userInput[1]);
            } else {
                System.out.println("Invalid input");
            }
        }
    }

    private static void registerTopic(String topic) throws IOException {
        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, EXCHANGE_NAME, topic);

        topicQueues.put(topic, queueName);

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(topic + ": '" + message + "'");
        };

        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
        });
    }

    private static void leaveTopic(String topic) throws IOException {
        String queueName = topicQueues.get(topic);
        if (queueName != null) {
            channel.queueDelete(queueName);
        }
    }
}
