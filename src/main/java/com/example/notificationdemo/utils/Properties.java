package com.example.notificationdemo.utils;

import java.io.FileInputStream;
import java.io.IOException;

/**
 * This utility class recovers the application.properties file like the automatism of Spring Framework does
 * but without having to use Spring beans.
 * Save your properties in the file: "src/main/resources/application.properties" in the form:
 * my.property.one=example
 * my.property.two=${ENV_VAR_NAME_1}
 * my.property.three=${ENV_VAR_NAME_2:default}
 *
 * Unlike Spring, it is not able to compose the properties this way:
 * my.property.four=${ENV_VAR_NAME_3:default}/the_path/continues/here
 *
 */
public class Properties {

    private static java.util.Properties prop;
    private static String PROPERTIES_FILE_PATH = "src/main/resources/application.properties";

    /**
     * Overwrite the properties filepath.
     *
     * @param filePath the new filepath
     */
    public static void setPropertiesFilePath(String filePath) {
        PROPERTIES_FILE_PATH = filePath;
    }

    /**
     * Returns a property value by specifying its name.
     * The property is fetched from "src/main/resources/application.properties"
     * or from another source if specified.
     *
     * @param propertyName the name of the property to be read from file
     * @return the value of the property
     */
    public static String get(String propertyName) {
        if (prop == null) {
            try {
                prop = readPropertiesFile(PROPERTIES_FILE_PATH);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        String property = prop.getProperty(propertyName);
        if (property == null) return null;
        if (property.matches("\\$\\{([^}]*)\\}")) {
            property = property.substring(2, property.length()-1);
            String[] pp = property.split(":");
            String envVarValue = System.getenv(pp[0]);
            if (envVarValue == null || "".equals(envVarValue)) {
                if (pp.length == 2) {
                    return pp[1];
                }
                return null;
            }
            return envVarValue;
        }
        return prop.getProperty(propertyName);
    }

    private static java.util.Properties readPropertiesFile(String fileName) throws IOException {
        FileInputStream fis = null;
        java.util.Properties prop = null;
        try {
            fis = new FileInputStream(fileName);
            prop = new java.util.Properties();
            prop.load(fis);
        } catch(IOException fnfe) {
            fnfe.printStackTrace();
        } finally {
            assert fis != null;
            fis.close();
        }
        return prop;
    }

}
