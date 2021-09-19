package ru.abenefic.rabbit;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

public class Producer {

    private static final String EXCHANGE_NAME = "topicsExchanger";
    private static final Scanner scanner = new Scanner(System.in);
    private static ConnectionFactory factory;

    public static void main(String[] args) throws IOException, TimeoutException {

        factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setUsername("admin");
        factory.setPassword("admin");

        System.out.println("Enter topic and message:");

        while (working()) {
            System.out.println("Enter topic and message:");
        }

    }

    private static boolean working() throws IOException, TimeoutException {

        String[] userInput = scanner.nextLine().split(" ");
        if (userInput.length == 1 && userInput[0].equals("exit")) {
            return false;
        } else if (userInput.length == 2) {
            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel()) {
                channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
                String topic = userInput[0];
                String message = userInput[1];

                channel.basicPublish(EXCHANGE_NAME, topic, null, message.getBytes(StandardCharsets.UTF_8));
            }
        }
        return true;
    }
}
