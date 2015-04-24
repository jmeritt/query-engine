package com.datadirect.platform;

import org.teiid.adminapi.Model;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.runtime.EmbeddedServer;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by jmeritt on 3/18/15.
 */
public class DataSource {
    public static final String USERNAME = "USERNAME";
    public static final String PASSWORD = "PASSWORD";
    public static final String ENDPOINT = "ENDPOINT";
    public static final String VIEWS_DDL = "VIEWSDDL";
    public static final String SECURITY_TYPE = "SECURITYTYPE";
    private String name;
    private String type;
    private Map<String, String> properties;

    public DataSource() {
        properties = new HashMap<>();
    }

    public DataSource(String name, String type, String username, String password) {
        properties = new HashMap<>();
        this.name = name;
        this.type = type;
        properties.put(USERNAME, username);
        properties.put(PASSWORD, password);
    }

    public DataSource(String name, String type, Map<String, String> properties) {
        this.name = name;
        this.type = type;
        this.properties = new HashMap(properties);
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getEndpoint() {
        return properties.get(ENDPOINT);
    }

    public void setEndpoint(String endpoint) {
        properties.put(ENDPOINT, endpoint);
    }

    public String getUsername() {
        return properties.get(USERNAME);
    }

    public void setUsername(String username) {
        properties.put(USERNAME, username);
    }

    public String getPassword() {
        return properties.get(PASSWORD);
    }

    public void setPassword(String password) {
        properties.put(PASSWORD, password);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void addProperty(String name, String value) {
        properties.put(name, value);
    }

    public String getProperty(String name) {
        return properties.get(name);
    }

    public String getSecurityType() {
        return properties.containsKey(SECURITY_TYPE) ? properties.get(SECURITY_TYPE) : "NONE";
    }

    public void setSecurityType(String type) {
        properties.put(SECURITY_TYPE, type);
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DataSource)) return false;

        DataSource that = (DataSource) o;

        if (!name.equals(that.name)) return false;
        return type.equals(that.type);

    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + type.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "DataSource{" +
                "name='" + name + '\'' +
                ", type='" + type + '\'' +
                '}';
    }

    protected ModelMetaData[] buildModelsAsMetadata(EmbeddedServer server, Object factory) {
        String boundName = getBoundName();
        server.addConnectionFactory(boundName, factory);

        //physical model
        ModelMetaData sourceModel = new ModelMetaData();
        sourceModel.setName(getName());
        for (String key : properties.keySet()) {
            if (!isKnownKey(key))
                sourceModel.addProperty(key, properties.get(key));
        }

        sourceModel.addSourceMapping("source-" + getName(), Translators.translatorForDSType(getType()), boundName);

        String viewsDefinition = properties.get(VIEWS_DDL);
        if (viewsDefinition == null)
            return new ModelMetaData[]{sourceModel};

        //virtual model
        ModelMetaData viewModel = new ModelMetaData();
        viewModel.setName(getName() + "View");
        viewModel.setModelType(Model.Type.VIRTUAL);
        viewModel.setSchemaSourceType("ddl");
        viewModel.setSchemaText(viewsDefinition);
        return new ModelMetaData[]{sourceModel, viewModel};
    }

    private boolean isKnownKey(String key) {
        return key.equals(USERNAME) ||
                key.equals(PASSWORD) ||
                key.equals(ENDPOINT) ||
                key.equals(SECURITY_TYPE) ||
                key.equals(VIEWS_DDL);
    }

    protected String getBoundName() {
        return String.format("java:/%s", getName());
    }


}
