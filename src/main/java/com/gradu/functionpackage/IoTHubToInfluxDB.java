package com.gradu.functionpackage;

import com.microsoft.azure.functions.ExecutionContext;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import java.util.concurrent.TimeUnit;
import com.microsoft.azure.functions.annotation.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Azure Functions with HTTP Trigger.
 */
public class IoTHubToInfluxDB {
    private static final String INFLUX_URL = "http://23.97.240.132:9096";
    private static final String INFLUX_DB = "environment_data";

    private static final InfluxDB influxDB = InfluxDBFactory.connect(INFLUX_URL);

    @FunctionName("IoTHubToInfluxDB")
    public void run(
            @EventHubTrigger(name = "message", eventHubName = "messages/events", connection = "IoTHubEndpoint", consumerGroup = "$Default") String[] messages,
            final ExecutionContext context) {

        context.getLogger().info("Received " + messages.length + " message(s) from IoT Hub.");

        influxDB.setDatabase(INFLUX_DB);

        context.getLogger().info("DB init successful, nice.");

        for (String message : messages) {
            try {
                // Assume incoming messages are JSON
                SensorData sensorData = parseSensorData(message);

                context.getLogger().info("Creating Point with the following data:");
                context.getLogger().info("Device ID: " + sensorData.getDeviceId());
                context.getLogger().info("Temperature: " + sensorData.getTemperature());
                context.getLogger().info("Humidity: " + sensorData.getHumidity());
                context.getLogger().info("Luminosity: " + sensorData.getLuminosity());
                context.getLogger().info("Pressure: " + sensorData.getPressure());

                Point point = Point.measurement("sensor_data")
                        .addField("deviceId", sensorData.getDeviceId())
                        .addField("temperature", sensorData.getTemperature())
                        .addField("humidity", sensorData.getHumidity())
                        .addField("luminosity", sensorData.getLuminosity())
                        .addField("pressure", sensorData.getPressure())
                        .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                        .build();

                influxDB.write(point);

                context.getLogger().info("Data written to InfluxDB: " + point.toString());
            } catch (Exception e) {
                context.getLogger().severe("Failed to process message: " + e.getMessage());
            }
        }
    }

    private SensorData parseSensorData(String message) throws JsonMappingException, JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(message, SensorData.class);
    }
}

class SensorData {
    private String deviceId;
    private float temperature;
    private float humidity;
    private float luminosity;
    private float pressure;

    // Default constructor
    public SensorData() {
    }

    // Constructor with parameters (optional)
    public SensorData(String deviceId, float temperature, float humidity, float luminosity, float pressure) {
        this.deviceId = deviceId;
        this.temperature = temperature;
        this.humidity = humidity;
        this.luminosity = luminosity;
        this.pressure = pressure;
    }

    // Getters and setters
    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public float getTemperature() {
        return temperature;
    }

    public void setTemperature(float temperature) {
        this.temperature = temperature;
    }

    public float getHumidity() {
        return humidity;
    }

    public void setHumidity(float humidity) {
        this.humidity = humidity;
    }

    public float getLuminosity() {
        return luminosity;
    }

    public void setLuminosity(float luminosity) {
        this.luminosity = luminosity;
    }

    public float getPressure() {
        return pressure;
    }

    public void setPressure(float pressure) {
        this.pressure = pressure;
    }
}