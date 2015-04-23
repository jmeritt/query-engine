package com.datadirect.platform;

import org.teiid.runtime.EmbeddedServer;
import org.w3c.dom.CDATASection;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.resource.ResourceException;
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


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DataSource)) return false;

        DataSource that = (DataSource) o;

        if (!name.equals(that.name)) return false;
        if (!type.equals(that.type)) return false;

        return true;
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

    protected Element[] buildModels(EmbeddedServer server, Document doc, Object factory) throws ResourceException {
        String boundName = getBoundName();
        server.addConnectionFactory(boundName, factory);

        Element sourceModel = doc.createElement("model");
        sourceModel.setAttribute("name", getName());
        for (String key : properties.keySet()) {
            if (!isKnownKey(key))
                sourceModel.appendChild(createPropertyElement(doc, key, properties.get(key)));
        }
        sourceModel.appendChild(createSource(doc, boundName));

        String viewsDefinition = properties.get(VIEWS_DDL);
        if (viewsDefinition == null)
            return new Element[]{sourceModel};
        Element viewModel = doc.createElement("model");
        viewModel.setAttribute("name", getName() + "View");
        viewModel.setAttribute("type", "VIRTUAL");
        Element metadata = doc.createElement("metadata");
        metadata.setAttribute("type", "DDL");
        CDATASection ddl = doc.createCDATASection(viewsDefinition);
        metadata.appendChild(ddl);
        viewModel.appendChild(metadata);

        return new Element[]{sourceModel, viewModel};

    }

    private boolean isKnownKey(String key) {
        return key.equals(USERNAME) ||
                key.equals(PASSWORD) ||
                key.equals(ENDPOINT) ||
                key.equals(VIEWS_DDL);
    }

    protected String getBoundName() {
        return String.format("java:/%s", getName());
    }

    protected Element createSource(Document doc, String jndiName) {
        Element source = doc.createElement("source");
        source.setAttribute("name", "source-" + getName());
        source.setAttribute("translator-name", Translators.translatorForDSType(getType()));
        source.setAttribute("connection-jndi-name", jndiName);
        return source;
    }

    protected Element createPropertyElement(Document doc, String name, String value) {
        Element property = doc.createElement("property");
        property.setAttribute("name", name);
        property.setAttribute("value", value);
        return property;
    }
}
