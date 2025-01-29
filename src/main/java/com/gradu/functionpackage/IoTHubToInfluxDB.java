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

public class IoTHubToInfluxDB {

    @FunctionName("IoTHubToInfluxDB")
    public void run(
            @EventHubTrigger(name = "message", eventHubName = "messages/events", connection = "IoTHubEndpoint", consumerGroup = "$Default") String[] messages,
            final ExecutionContext context) {

        String INFLUX_URL = System.getenv("INFLUX_URL");
        String INFLUX_DB = System.getenv("INFLUX_DB");
        String INFLUX_USER = System.getenv("INFLUX_USER");
        String INFLUX_PASSWORD = System.getenv("INFLUX_PASSWORD");

        if (INFLUX_URL == null || INFLUX_DB == null || INFLUX_USER == null || INFLUX_PASSWORD == null) {
            context.getLogger().severe("Required environment variables are not set");
            return;
        }

        InfluxDB influxDB = InfluxDBFactory.connect(INFLUX_URL, INFLUX_USER, INFLUX_PASSWORD);
        influxDB.setDatabase(INFLUX_DB);

        for (String message : messages) {
            try {
                SensorData sensorData = parseSensorData(message);

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

    public SensorData() {
    }

    public SensorData(String deviceId, float temperature, float humidity, float luminosity, float pressure) {
        this.deviceId = deviceId;
        this.temperature = temperature;
        this.humidity = humidity;
        this.luminosity = luminosity;
        this.pressure = pressure;
    }

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